"""
Census Log Reader Agent
=======================
Agent for reading and summarizing Census pipeline logs.

Functions:
1. Reads all Census agent log files for the day
2. Parses structured JSON log entries
3. Generates daily summary JSON
4. Counts: records collected, transformed, errors, warnings
5. Prepares summary for email agent

Logs are read from: logs/census/*.log
"""

import json
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from agents.base.census_base_agent import (
    CensusBaseAgent, PipelineLayer, EventType, LogEntry,
    get_today_log_files, parse_log_file, summarize_log_entries,
    CENSUS_LOGS_DIR
)


class CensusLogReaderAgent(CensusBaseAgent):
    """
    Agent for reading and summarizing Census pipeline logs.

    Reads JSON log files from logs/census/ and creates a daily summary.
    """

    def __init__(self, **kwargs):
        super().__init__(agent_name='CensusLogReader', **kwargs)

    def get_layer(self) -> PipelineLayer:
        # Log reader is a monitoring agent, not tied to a specific layer
        return PipelineLayer.GOLD

    def run(
        self,
        target_date: date = None,
        include_all_agents: bool = True
    ):
        """
        Read and summarize Census pipeline logs.

        Args:
            target_date: Date to summarize (default: today)
            include_all_agents: Include all Census agents in summary

        Returns:
            AgentResult with summary data
        """
        target_date = target_date or date.today()

        self.log_event(
            EventType.INFO,
            f"Reading Census logs for {target_date}",
            data={'target_date': str(target_date)}
        )

        # Find all log files for the date
        log_files = self._find_log_files(target_date)

        if not log_files:
            self.log_warning(
                f"No log files found for {target_date}",
                data={'logs_dir': str(CENSUS_LOGS_DIR)}
            )
            return self.complete()

        self.log_event(
            EventType.INFO,
            f"Found {len(log_files)} log files",
            data={'files': [f.name for f in log_files]}
        )

        # Parse all log files
        all_entries = []
        for log_file in log_files:
            try:
                entries = parse_log_file(log_file)
                all_entries.extend(entries)
                self.add_records_processed(len(entries))

                self.log_event(
                    EventType.INFO,
                    f"Parsed {len(entries)} entries from {log_file.name}",
                    data={'file': log_file.name, 'entries': len(entries)}
                )
            except Exception as e:
                self.log_warning(f"Error parsing {log_file.name}: {e}")

        # Generate summary
        summary = self._generate_summary(all_entries, target_date)

        # Save summary to file
        summary_path = self._save_summary(summary, target_date)

        self.set_metadata('summary', summary)
        self.set_metadata('summary_path', str(summary_path))
        self.set_metadata('total_entries', len(all_entries))
        self.set_metadata('log_files_count', len(log_files))

        return self.complete()

    def _find_log_files(self, target_date: date) -> List[Path]:
        """Find all log files for a given date"""
        if not CENSUS_LOGS_DIR.exists():
            return []

        date_str = target_date.isoformat()
        pattern = f"*_{date_str}.log"

        return list(CENSUS_LOGS_DIR.glob(pattern))

    def _generate_summary(
        self,
        entries: List[LogEntry],
        target_date: date
    ) -> Dict[str, Any]:
        """Generate summary from log entries"""
        # Use the utility function
        base_summary = summarize_log_entries(entries)

        # Enhance with pipeline-specific info
        summary = {
            'date': str(target_date),
            'generated_at': datetime.now().isoformat(),
            'total_events': base_summary['total_events'],
            'pipeline_status': self._determine_pipeline_status(base_summary),
            'layers': base_summary['layers'],
            'agents': base_summary['agents'],
            'verifications': base_summary['verifications'],
            'errors': base_summary['errors'],
            'warnings': base_summary['warnings'],
            'records': self._extract_record_counts(entries),
        }

        return summary

    def _determine_pipeline_status(self, summary: Dict) -> str:
        """Determine overall pipeline status"""
        error_count = len(summary.get('errors', []))
        warning_count = len(summary.get('warnings', []))

        # Check if any agents failed
        failed_agents = [
            name for name, info in summary.get('agents', {}).items()
            if info.get('success') is False
        ]

        if error_count > 0 or failed_agents:
            return 'FAILED'
        elif warning_count > 5:
            return 'WARNING'
        elif summary.get('total_events', 0) == 0:
            return 'NO_DATA'
        else:
            return 'SUCCESS'

    def _extract_record_counts(self, entries: List[LogEntry]) -> Dict[str, int]:
        """Extract record counts from log entries"""
        counts = {
            'bronze_collected': 0,
            'silver_transformed': 0,
            'gold_created': 0,
            'verifications_passed': 0,
            'verifications_failed': 0,
        }

        for entry in entries:
            data = entry.data or {}

            # Extract from agent_complete events
            if entry.event_type == 'agent_complete':
                if entry.layer == 'bronze':
                    counts['bronze_collected'] += data.get('records_processed', 0)
                elif entry.layer == 'silver':
                    counts['silver_transformed'] += data.get('records_processed', 0)
                elif entry.layer == 'gold':
                    counts['gold_created'] += data.get('views_created', 0)

                counts['verifications_passed'] += data.get('checks_passed', 0)
                counts['verifications_failed'] += data.get('checks_failed', 0)

        return counts

    def _save_summary(self, summary: Dict, target_date: date) -> Path:
        """Save summary to JSON file"""
        summary_dir = CENSUS_LOGS_DIR / 'summaries'
        summary_dir.mkdir(parents=True, exist_ok=True)

        summary_path = summary_dir / f"daily_summary_{target_date}.json"

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)

        self.log_event(
            EventType.DATA_SAVE,
            f"Saved summary to {summary_path.name}",
            data={'path': str(summary_path)}
        )

        return summary_path

    def get_latest_summary(self) -> Optional[Dict]:
        """Get the most recent daily summary"""
        summary_dir = CENSUS_LOGS_DIR / 'summaries'

        if not summary_dir.exists():
            return None

        summaries = sorted(summary_dir.glob("daily_summary_*.json"), reverse=True)

        if not summaries:
            return None

        with open(summaries[0], 'r', encoding='utf-8') as f:
            return json.load(f)


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for Census Log Reader Agent"""
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='Census Log Reader Agent')

    parser.add_argument(
        '--date',
        help='Target date (YYYY-MM-DD), defaults to today'
    )

    args = parser.parse_args()

    # Parse date
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()

    agent = CensusLogReaderAgent()
    result = agent.run(target_date=target_date)

    print(f"\nResult: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Log entries processed: {result.records_processed}")
    print(f"Duration: {result.duration_seconds:.1f}s")

    # Print summary
    summary = result.metadata.get('summary', {})
    if summary:
        print(f"\n{'='*60}")
        print(f"DAILY SUMMARY: {summary.get('date', 'N/A')}")
        print(f"{'='*60}")
        print(f"Pipeline Status: {summary.get('pipeline_status', 'UNKNOWN')}")
        print(f"Total Events: {summary.get('total_events', 0)}")

        records = summary.get('records', {})
        print(f"\nRecords:")
        print(f"  Bronze collected: {records.get('bronze_collected', 0)}")
        print(f"  Silver transformed: {records.get('silver_transformed', 0)}")
        print(f"  Gold views created: {records.get('gold_created', 0)}")

        verifications = summary.get('verifications', {})
        print(f"\nVerifications:")
        print(f"  Passed: {verifications.get('passed', 0)}")
        print(f"  Failed: {verifications.get('failed', 0)}")

        errors = summary.get('errors', [])
        if errors:
            print(f"\nErrors ({len(errors)}):")
            for e in errors[:5]:
                print(f"  - [{e.get('agent', '?')}] {e.get('message', 'Unknown')}")


if __name__ == '__main__':
    main()
