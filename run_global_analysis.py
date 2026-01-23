#!/usr/bin/env python3
"""
Global Snowfall Analysis - Master Orchestrator
Complete end-to-end global snowfall prediction system for Phelps and Land O'Lakes, WI

This script orchestrates:
1. Global data collection (85 years, worldwide locations)
2. Cross-regional correlation analysis
3. Lag pattern identification
4. Prediction model generation
5. Visualization and reporting

Usage:
    # Full pipeline (will take several hours for initial data collection)
    python run_global_analysis.py --full

    # Just analysis (if data already collected)
    python run_global_analysis.py --analyze-only

    # Quick summary
    python run_global_analysis.py --summary
"""

import os
import sys
import json
import subprocess
from datetime import datetime
import argparse


class GlobalAnalysisOrchestrator:
    """Orchestrate complete global snowfall analysis pipeline"""

    def __init__(self, db_path: str = "global_snowfall.db"):
        self.db_path = db_path
        self.model_file = "phelps_prediction_model.json"
        self.report_file = "global_analysis_report.txt"

    def step1_collect_data(self, start_date: str = "1940-01-01", rate_limit: float = 1.0):
        """Step 1: Collect global historical data"""
        print(f"\n{'#'*80}")
        print(f"# STEP 1: GLOBAL DATA COLLECTION")
        print(f"# Database: {self.db_path}")
        print(f"# Period: {start_date} to present")
        print(f"{'#'*80}\n")

        cmd = [
            "python3", "global_snowfall_fetcher.py",
            "--db", self.db_path,
            "--start", start_date,
            "--rate-limit", str(rate_limit)
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n✗ Data collection failed!")
            sys.exit(1)

        print(f"\n✓ Data collection complete!")

    def step2_analyze_correlations(self, max_lag_days: int = 30):
        """Step 2: Analyze global correlations"""
        print(f"\n{'#'*80}")
        print(f"# STEP 2: CORRELATION ANALYSIS")
        print(f"# Target: Northern Wisconsin (Phelps/Land O'Lakes)")
        print(f"# Max Lag: {max_lag_days} days")
        print(f"{'#'*80}\n")

        cmd = [
            "python3", "global_correlation_analysis.py",
            "--db", self.db_path,
            "--target", "northern_wisconsin",
            "--max-lag", str(max_lag_days),
            "--export",
            "--model",
            "--model-file", self.model_file
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n✗ Correlation analysis failed!")
            sys.exit(1)

        print(f"\n✓ Correlation analysis complete!")

    def step3_generate_visualizations(self):
        """Step 3: Generate visualizations"""
        print(f"\n{'#'*80}")
        print(f"# STEP 3: VISUALIZATION GENERATION")
        print(f"{'#'*80}\n")

        cmd = [
            "python3", "visualize_global_snowfall.py",
            "--db", self.db_path,
            "--model", self.model_file
        ]

        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"\n⚠ Visualization generation failed (optional step)")
        else:
            print(f"\n✓ Visualizations generated!")

    def step4_generate_report(self):
        """Step 4: Generate comprehensive analysis report"""
        print(f"\n{'#'*80}")
        print(f"# STEP 4: REPORT GENERATION")
        print(f"{'#'*80}\n")

        report_lines = []
        report_lines.append("="*80)
        report_lines.append("GLOBAL SNOWFALL ANALYSIS REPORT")
        report_lines.append(f"Target: Phelps and Land O'Lakes, Wisconsin")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("="*80)
        report_lines.append("")

        # Load prediction model
        if os.path.exists(self.model_file):
            with open(self.model_file, 'r') as f:
                model = json.load(f)

            report_lines.append("PREDICTION MODEL SUMMARY")
            report_lines.append("-"*80)
            report_lines.append(f"Number of Global Predictors: {model['num_predictors']}")
            report_lines.append("")
            report_lines.append("Top 10 Global Snowfall Predictors for Northern Wisconsin:")
            report_lines.append("")

            for i, pred in enumerate(model['predictors'][:10], 1):
                region = pred['region'].replace('_', ' ').title()
                corr = pred['correlation']
                lag = pred['lag_days']
                weight = pred['normalized_weight']
                sig = "***" if pred['significant'] else ""

                if lag > 0:
                    lag_text = f"Leads WI by {lag} days"
                elif lag < 0:
                    lag_text = f"Follows WI by {abs(lag)} days"
                else:
                    lag_text = "Simultaneous"

                report_lines.append(f"{i:2d}. {region:30s}")
                report_lines.append(f"    Correlation: {corr:+.3f} {sig}")
                report_lines.append(f"    Lag Pattern: {lag_text}")
                report_lines.append(f"    Model Weight: {weight:.1%}")
                report_lines.append("")

            report_lines.append("="*80)
            report_lines.append("KEY INSIGHTS")
            report_lines.append("-"*80)
            report_lines.append("")

            # Identify key predictor categories
            russia_predictors = [p for p in model['predictors'] if 'russia' in p['region'] or 'siberia' in p['region']]
            west_us_predictors = [p for p in model['predictors'] if 'california' in p['region'] or 'colorado' in p['region']]
            japan_predictors = [p for p in model['predictors'] if 'japan' in p['region']]
            china_predictors = [p for p in model['predictors'] if 'china' in p['region']]

            if russia_predictors:
                report_lines.append("RUSSIA/SIBERIA CONNECTIONS:")
                avg_lag = sum(p['lag_days'] for p in russia_predictors) / len(russia_predictors)
                report_lines.append(f"  {len(russia_predictors)} regions show correlation")
                report_lines.append(f"  Average lag: {avg_lag:.1f} days (Siberian cold air source)")
                report_lines.append("")

            if west_us_predictors:
                report_lines.append("WESTERN US (CA/CO) CONNECTIONS:")
                avg_lag = sum(p['lag_days'] for p in west_us_predictors) / len(west_us_predictors)
                report_lines.append(f"  {len(west_us_predictors)} regions show correlation")
                report_lines.append(f"  Average lag: {avg_lag:.1f} days (Pacific pattern indicator)")
                report_lines.append("")

            if japan_predictors:
                report_lines.append("JAPAN CONNECTIONS:")
                avg_lag = sum(p['lag_days'] for p in japan_predictors) / len(japan_predictors)
                report_lines.append(f"  {len(japan_predictors)} regions show correlation")
                report_lines.append(f"  Average lag: {avg_lag:.1f} days (East Asian jet stream)")
                report_lines.append("")

            if china_predictors:
                report_lines.append("CHINA CONNECTIONS:")
                avg_lag = sum(p['lag_days'] for p in china_predictors) / len(china_predictors)
                report_lines.append(f"  {len(china_predictors)} regions show correlation")
                report_lines.append(f"  Average lag: {avg_lag:.1f} days (Tibetan High influence)")
                report_lines.append("")

        report_lines.append("="*80)

        # Write report
        report_text = "\n".join(report_lines)
        with open(self.report_file, 'w') as f:
            f.write(report_text)

        print(report_text)
        print(f"\n✓ Report saved to: {self.report_file}")

    def show_summary(self):
        """Show quick summary of existing data"""
        print(f"\n{'='*80}")
        print(f"GLOBAL SNOWFALL ANALYSIS - QUICK SUMMARY")
        print(f"{'='*80}\n")

        if not os.path.exists(self.db_path):
            print(f"✗ Database not found: {self.db_path}")
            print(f"  Run with --full to collect data\n")
            return

        cmd = ["python3", "global_snowfall_fetcher.py", "--db", self.db_path, "--summary"]
        subprocess.run(cmd)

        if os.path.exists(self.model_file):
            print(f"\n{'='*80}")
            print(f"PREDICTION MODEL")
            print(f"{'='*80}\n")

            with open(self.model_file, 'r') as f:
                model = json.load(f)

            print(f"Target: {model['target_region'].replace('_', ' ').title()}")
            print(f"Predictors: {model['num_predictors']}")
            print(f"Created: {model['created_at']}")
            print(f"\nTop 5 Predictors:")
            for i, pred in enumerate(model['predictors'][:5], 1):
                print(f"  {i}. {pred['region'].replace('_', ' ').title():30s} | r={pred['correlation']:+.3f} | lag={pred['lag_days']:+3d}d")
            print("")

    def run_full_pipeline(self, start_date: str = "1940-01-01", max_lag_days: int = 30, rate_limit: float = 1.0):
        """Run complete analysis pipeline"""
        print(f"\n{'#'*80}")
        print(f"# GLOBAL SNOWFALL ANALYSIS - FULL PIPELINE")
        print(f"# Target: Phelps and Land O'Lakes, Wisconsin")
        print(f"# Period: {start_date} to present")
        print(f"{'#'*80}\n")

        start_time = datetime.now()

        # Step 1: Data Collection
        if not os.path.exists(self.db_path):
            self.step1_collect_data(start_date, rate_limit)
        else:
            print(f"\n✓ Database already exists: {self.db_path}")
            print(f"  Skipping data collection (use --force-collect to re-fetch)\n")

        # Step 2: Correlation Analysis
        self.step2_analyze_correlations(max_lag_days)

        # Step 3: Visualizations (optional)
        if os.path.exists("visualize_global_snowfall.py"):
            self.step3_generate_visualizations()

        # Step 4: Report
        self.step4_generate_report()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n{'#'*80}")
        print(f"# ✓ PIPELINE COMPLETE")
        print(f"# Duration: {duration/60:.1f} minutes")
        print(f"# Output Files:")
        print(f"#   - Database: {self.db_path}")
        print(f"#   - Model: {self.model_file}")
        print(f"#   - Report: {self.report_file}")
        print(f"{'#'*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Global Snowfall Analysis for Phelps/Land O\'Lakes, WI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete analysis (first time - will take several hours)
  python run_global_analysis.py --full

  # Just run analysis on existing data
  python run_global_analysis.py --analyze-only

  # Show summary of existing data
  python run_global_analysis.py --summary

  # Re-collect data with custom date range
  python run_global_analysis.py --full --start 2000-01-01 --force-collect
        """
    )

    parser.add_argument('--full', action='store_true', help='Run complete pipeline (data + analysis)')
    parser.add_argument('--analyze-only', action='store_true', help='Run analysis only (skip data collection)')
    parser.add_argument('--summary', action='store_true', help='Show quick summary')
    parser.add_argument('--db', default='global_snowfall.db', help='Database file path')
    parser.add_argument('--start', default='1940-01-01', help='Start date for data collection')
    parser.add_argument('--max-lag', type=int, default=30, help='Maximum lag days for correlation analysis')
    parser.add_argument('--rate-limit', type=float, default=1.0, help='Seconds between API calls')
    parser.add_argument('--force-collect', action='store_true', help='Force re-collection even if DB exists')

    args = parser.parse_args()

    orchestrator = GlobalAnalysisOrchestrator(db_path=args.db)

    if args.summary:
        orchestrator.show_summary()
    elif args.analyze_only:
        if not os.path.exists(args.db):
            print(f"✗ Database not found: {args.db}")
            print(f"  Run with --full to collect data first")
            sys.exit(1)
        orchestrator.step2_analyze_correlations(args.max_lag)
        orchestrator.step4_generate_report()
    elif args.full:
        if args.force_collect and os.path.exists(args.db):
            os.remove(args.db)
            print(f"✓ Removed existing database for fresh collection")
        orchestrator.run_full_pipeline(args.start, args.max_lag, args.rate_limit)
    else:
        parser.print_help()
        print(f"\nQuick Start:")
        print(f"  python run_global_analysis.py --summary    # Check current state")
        print(f"  python run_global_analysis.py --full       # Run complete analysis")


if __name__ == '__main__':
    main()
