"""
Security Agents Helper
======================

Lightweight AST-based security scanners for Python code.
Detects common security pitfalls in API-oriented code.

Usage:
    python agents.py /path/to/source

Checks:
- Hardcoded secrets (API keys, passwords, tokens)
- Dynamic code execution (eval, exec, compile)
- Debug mode enabled
- Plain HTTP requests
- Broad exception handlers
"""

import ast
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class SecurityIssue:
    """Represents a security issue found in code"""
    check_name: str
    severity: str  # HIGH, MEDIUM, LOW
    file_path: str
    line_number: int
    code_snippet: str
    description: str
    suggested_fix: str


class SecurityAgent(ast.NodeVisitor):
    """Base class for security check agents"""

    def __init__(self, file_path: str, source_code: str):
        self.file_path = file_path
        self.source_code = source_code
        self.source_lines = source_code.split('\n')
        self.issues: List[SecurityIssue] = []

    def get_code_snippet(self, lineno: int, context: int = 2) -> str:
        """Get code snippet around a line number"""
        start = max(0, lineno - context - 1)
        end = min(len(self.source_lines), lineno + context)
        lines = []

        for i in range(start, end):
            prefix = ">>> " if i == lineno - 1 else "    "
            lines.append(f"{prefix}{i+1:4d}: {self.source_lines[i]}")

        return '\n'.join(lines)

    def add_issue(self, check_name: str, severity: str, lineno: int,
                  description: str, suggested_fix: str):
        """Add a security issue"""
        issue = SecurityIssue(
            check_name=check_name,
            severity=severity,
            file_path=self.file_path,
            line_number=lineno,
            code_snippet=self.get_code_snippet(lineno),
            description=description,
            suggested_fix=suggested_fix
        )
        self.issues.append(issue)


class HardcodedSecretsAgent(SecurityAgent):
    """Detects hardcoded secrets in code"""

    SECRET_PATTERNS = [
        'api_key', 'apikey', 'api_token', 'token',
        'password', 'passwd', 'pwd',
        'secret', 'secret_key',
        'access_key', 'private_key',
        'auth_token', 'bearer'
    ]

    def visit_Assign(self, node):
        """Check assignment statements for hardcoded secrets"""
        for target in node.targets:
            if isinstance(target, ast.Name):
                var_name = target.id.lower()

                # Check if variable name suggests it's a secret
                if any(pattern in var_name for pattern in self.SECRET_PATTERNS):
                    # Check if assigned a string literal
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        value = node.value.value

                        # Skip if it's a placeholder or empty
                        if value and not any(x in value.upper() for x in ['YOUR_', 'REPLACE', 'EXAMPLE', 'XXX', 'TODO']):
                            self.add_issue(
                                check_name="Hardcoded Secret",
                                severity="HIGH",
                                lineno=node.lineno,
                                description=f"Hardcoded secret found: {target.id} = \"{value[:20]}...\"",
                                suggested_fix=f"Move to environment variable:\n    {target.id} = os.getenv('{target.id.upper()}')"
                            )

        self.generic_visit(node)


class DynamicExecutionAgent(SecurityAgent):
    """Detects dangerous dynamic code execution"""

    DANGEROUS_FUNCTIONS = ['eval', 'exec', 'compile', '__import__']

    def visit_Call(self, node):
        """Check for dangerous function calls"""
        func_name = None

        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name in self.DANGEROUS_FUNCTIONS:
            self.add_issue(
                check_name="Dynamic Code Execution",
                severity="HIGH",
                lineno=node.lineno,
                description=f"Dangerous function '{func_name}()' allows arbitrary code execution",
                suggested_fix=f"Avoid using '{func_name}()'. Use safer alternatives like ast.literal_eval() for data parsing or importlib for imports."
            )

        self.generic_visit(node)


class DebugModeAgent(SecurityAgent):
    """Detects debug mode enabled in production code"""

    def visit_Assign(self, node):
        """Check for debug = True assignments"""
        for target in node.targets:
            if isinstance(target, ast.Attribute):
                if target.attr == 'debug':
                    if isinstance(node.value, ast.Constant) and node.value.value is True:
                        self.add_issue(
                            check_name="Debug Mode Enabled",
                            severity="HIGH",
                            lineno=node.lineno,
                            description="Debug mode is enabled (app.debug = True)",
                            suggested_fix="Set debug=False in production. Use environment variable:\n    app.debug = os.getenv('DEBUG', 'False') == 'True'"
                        )

        self.generic_visit(node)

    def visit_Call(self, node):
        """Check for app.run(debug=True)"""
        if isinstance(node.func, ast.Attribute):
            if node.func.attr == 'run':
                for keyword in node.keywords:
                    if keyword.arg == 'debug':
                        if isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                            self.add_issue(
                                check_name="Debug Mode Enabled",
                                severity="HIGH",
                                lineno=node.lineno,
                                description="Debug mode enabled in app.run(debug=True)",
                                suggested_fix="Remove debug=True or use environment variable"
                            )

        self.generic_visit(node)


class PlainHTTPAgent(SecurityAgent):
    """Detects plain HTTP requests (should use HTTPS)"""

    def visit_Call(self, node):
        """Check for HTTP requests without TLS"""
        # Check for requests.get/post/etc with http:// URLs
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in ['get', 'post', 'put', 'delete', 'patch', 'request']:
                # Check first argument (URL)
                if node.args:
                    arg = node.args[0]
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        if arg.value.startswith('http://'):
                            self.add_issue(
                                check_name="Plain HTTP Request",
                                severity="MEDIUM",
                                lineno=node.lineno,
                                description=f"Unencrypted HTTP request to: {arg.value[:50]}...",
                                suggested_fix="Use HTTPS instead of HTTP unless the endpoint is intentionally non-TLS"
                            )

        self.generic_visit(node)


class BroadExceptionAgent(SecurityAgent):
    """Detects overly broad exception handlers"""

    def visit_ExceptHandler(self, node):
        """Check for broad exception catching"""
        if node.type:
            exception_type = None

            if isinstance(node.type, ast.Name):
                exception_type = node.type.id

            if exception_type in ['Exception', 'BaseException']:
                # Check if there's any logging in the handler
                has_logging = False
                for child in ast.walk(node):
                    if isinstance(child, ast.Call):
                        if isinstance(child.func, ast.Attribute):
                            if child.func.attr in ['debug', 'info', 'warning', 'error', 'critical', 'exception']:
                                has_logging = True
                                break

                severity = "MEDIUM" if has_logging else "HIGH"
                description = f"Broad exception handler catches {exception_type}"

                if not has_logging:
                    description += " without logging"

                self.add_issue(
                    check_name="Broad Exception Handler",
                    severity=severity,
                    lineno=node.lineno,
                    description=description,
                    suggested_fix=f"Catch specific exceptions instead of {exception_type}. Always log errors:\n    except SpecificError as e:\n        logger.error(f'Error: {{e}}')\n        raise"
                )

        self.generic_visit(node)


class SecurityScanner:
    """Main scanner that runs all security agents"""

    AGENTS = [
        HardcodedSecretsAgent,
        DynamicExecutionAgent,
        DebugModeAgent,
        PlainHTTPAgent,
        BroadExceptionAgent
    ]

    def __init__(self, target_path: str):
        self.target_path = Path(target_path)
        self.all_issues: List[SecurityIssue] = []
        self.stats: Dict[str, int] = defaultdict(int)

    def scan_file(self, file_path: Path) -> List[SecurityIssue]:
        """Scan a single Python file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source_code = f.read()

            tree = ast.parse(source_code, filename=str(file_path))

            file_issues = []
            for AgentClass in self.AGENTS:
                agent = AgentClass(str(file_path), source_code)
                agent.visit(tree)
                file_issues.extend(agent.issues)

            return file_issues

        except SyntaxError as e:
            print(f"⚠️  Syntax error in {file_path}: {e}")
            return []
        except Exception as e:
            print(f"⚠️  Error scanning {file_path}: {e}")
            return []

    def scan_directory(self) -> List[SecurityIssue]:
        """Scan all Python files in directory"""
        python_files = []

        if self.target_path.is_file():
            python_files = [self.target_path]
        else:
            python_files = list(self.target_path.rglob("*.py"))

        print(f"🔍 Scanning {len(python_files)} Python files in {self.target_path}...")
        print()

        for py_file in python_files:
            # Skip virtual environments and common ignored directories
            if any(part in py_file.parts for part in ['venv', 'env', '.venv', 'site-packages', '__pycache__']):
                continue

            issues = self.scan_file(py_file)
            self.all_issues.extend(issues)

            if issues:
                print(f"📄 {py_file.relative_to(self.target_path.parent if self.target_path.is_file() else self.target_path)}: {len(issues)} issue(s)")

        # Collect statistics
        for issue in self.all_issues:
            self.stats[issue.check_name] += 1
            self.stats[f"severity_{issue.severity}"] += 1

        return self.all_issues

    def print_report(self):
        """Print detailed security report"""
        print()
        print("=" * 80)
        print("SECURITY SCAN REPORT")
        print("=" * 80)
        print()

        if not self.all_issues:
            print("✅ No security issues found!")
            print()
            return

        # Group issues by severity
        high_issues = [i for i in self.all_issues if i.severity == "HIGH"]
        medium_issues = [i for i in self.all_issues if i.severity == "MEDIUM"]
        low_issues = [i for i in self.all_issues if i.severity == "LOW"]

        print(f"Total Issues: {len(self.all_issues)}")
        print(f"  🔴 HIGH:   {len(high_issues)}")
        print(f"  🟡 MEDIUM: {len(medium_issues)}")
        print(f"  🟢 LOW:    {len(low_issues)}")
        print()

        print("Issues by Type:")
        print("-" * 80)
        for check_name in sorted(set(i.check_name for i in self.all_issues)):
            count = self.stats[check_name]
            print(f"  • {check_name}: {count}")
        print()

        # Print detailed issues
        print("=" * 80)
        print("DETAILED FINDINGS")
        print("=" * 80)
        print()

        for issue in sorted(self.all_issues, key=lambda x: (x.severity != "HIGH", x.severity != "MEDIUM", x.file_path, x.line_number)):
            severity_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}[issue.severity]

            print(f"{severity_icon} [{issue.severity}] {issue.check_name}")
            print(f"File: {issue.file_path}:{issue.line_number}")
            print(f"Description: {issue.description}")
            print()
            print(issue.code_snippet)
            print()
            print(f"💡 Suggested Fix:")
            print(f"   {issue.suggested_fix}")
            print()
            print("-" * 80)
            print()

    def export_json(self, output_file: str = "security_report.json"):
        """Export report as JSON"""
        import json

        report = {
            "summary": {
                "total_issues": len(self.all_issues),
                "high": self.stats["severity_HIGH"],
                "medium": self.stats["severity_MEDIUM"],
                "low": self.stats["severity_LOW"],
                "scanned_path": str(self.target_path)
            },
            "issues": [
                {
                    "check": issue.check_name,
                    "severity": issue.severity,
                    "file": issue.file_path,
                    "line": issue.line_number,
                    "description": issue.description,
                    "fix": issue.suggested_fix
                }
                for issue in self.all_issues
            ]
        }

        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"📊 Report exported to {output_file}")


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python agents.py /path/to/source [--json]")
        print()
        print("Examples:")
        print("  python agents.py .")
        print("  python agents.py noaa_weather_fetcher.py")
        print("  python agents.py . --json")
        sys.exit(1)

    target_path = sys.argv[1]
    export_json = "--json" in sys.argv

    if not os.path.exists(target_path):
        print(f"❌ Path does not exist: {target_path}")
        sys.exit(1)

    scanner = SecurityScanner(target_path)
    scanner.scan_directory()
    scanner.print_report()

    if export_json:
        scanner.export_json()

    # Exit with error code if high severity issues found
    high_count = scanner.stats.get("severity_HIGH", 0)
    if high_count > 0:
        print(f"❌ {high_count} HIGH severity issue(s) found!")
        sys.exit(1)
    else:
        print("✅ No HIGH severity issues found.")
        sys.exit(0)


if __name__ == "__main__":
    main()
