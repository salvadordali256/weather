## Security Agents Helper

`agents.py` provides lightweight scanners that review API-oriented Python code for obvious security pitfalls before deployment. Each detector ("agent") walks the AST of every `.py` file under a target directory and reports where remediation may be needed.

### Checks Included
- **Hardcoded secrets** – flags assignments like `API_KEY = "123"`; move these values into environment variables or a secret manager.
- **Dynamic code execution** – warns when `eval`, `exec`, or `compile` are invoked because they often expose arbitrary-code vulnerabilities.
- **Debug mode enabled** – detects `app.debug = True` and `app.run(debug=True)` that should never be shipped to production environments.
- **Plain HTTP requests** – highlights calls such as `requests.get("http://...")`; switch to HTTPS unless the endpoint is intentionally non‑TLS.
- **Broad exception handlers** – finds `except Exception` / `except BaseException` blocks that swallow errors; catch specific exceptions and log them.

### Running the Scanner
```bash
python agents.py /path/to/api/source
```

The script prints each issue with the check name, file, line number, context, and a suggested fix. Use the output to prioritize hardening work or pair the tool with CI to prevent regressions.
