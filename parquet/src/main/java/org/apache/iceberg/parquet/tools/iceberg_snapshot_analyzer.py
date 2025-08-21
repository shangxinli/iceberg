#!/usr/bin/env python3
"""
Iceberg Snapshot Analyzer Tool

This tool analyzes Iceberg table metadata to extract operation information from the latest snapshot.
It supports various storage systems including GCS, HDFS, and local filesystem.

USAGE:
    python3 iceberg_snapshot_analyzer.py <table_path>
    python3 iceberg_snapshot_analyzer.py --json <table_path>
    python3 iceberg_snapshot_analyzer.py --help

EXAMPLES:
    # Analyze a GCS-based Iceberg table (uses HDFS connector)
    python3 iceberg_snapshot_analyzer.py gs://bucket/path/to/table/
    
    # Analyze an HDFS-based Iceberg table
    python3 iceberg_snapshot_analyzer.py hdfs://namenode/path/to/table/
    
    # Output as JSON for programmatic use
    python3 iceberg_snapshot_analyzer.py --json gs://bucket/path/to/table/
    
    # Extract specific fields using jq
    python3 iceberg_snapshot_analyzer.py --json gs://bucket/table/ | jq '.operation'
    python3 iceberg_snapshot_analyzer.py --json gs://bucket/table/ | jq '.records.total'

OUTPUT INFORMATION:
    The tool provides the following metrics from the latest snapshot:
    
    - Operation Type: append, overwrite, upsert, delete, etc.
    - Files Information:
        * Files Inserted: Number and size of newly added files
        * Files Updated: Number and size of modified files  
        * Files Deleted: Number and size of removed files
        * Total Files: Current total files and size
    - Records Information:
        * Records Inserted/Updated/Deleted
        * Total Records: Current total record count
    - Partitions Written: Number of partitions affected
    - Delete Files: Position and equality deletes (if any)
    - Operation Duration: Time taken (when available)
    - Spark App ID: ID of the Spark job
    - Timestamp: When the operation completed

EXAMPLE OUTPUT (UPSERT):
    ============================================================
    ICEBERG TABLE SNAPSHOT ANALYSIS
    ============================================================
    
    Table Location: gs://bucket/path/to/table
    Snapshot ID: 7890123456789012345
    Operation: UPSERT
    Timestamp: 2023-08-20T16:00:00
    
    ----------------------------------------
    FILES INFORMATION
    ----------------------------------------
    Files Inserted: 12 (2.50 MB)
    Files Updated: 5 (1.25 MB)
    Files Deleted: 2 (256.00 KB)
    Total Files: 30 (12.25 MB)
    
    ----------------------------------------
    RECORDS INFORMATION
    ----------------------------------------
    Records Inserted: 50,000
    Records Updated: 25,000
    Records Deleted: 5,000
    Total Records: 250,000
    
    Partitions Written: 3
    Operation Duration: 12h 0m 0s
    ============================================================

NOTES:
    - For GCS paths, uses 'hdfs dfs' commands with organization's HDFS-to-GCS connector
    - Reads only metadata files, not data files (very fast)
    - Uses version-hint.text for optimal performance when available
"""

import json
import sys
import argparse
import subprocess
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from urllib.parse import urlparse
import os


class IcebergSnapshotAnalyzer:
    def __init__(self, table_path: str):
        """
        Initialize the analyzer with the Iceberg table path.
        
        Args:
            table_path: Path to the Iceberg table (e.g., gs://bucket/path/to/table/)
        """
        self.table_path = table_path.rstrip('/')
        self.metadata_path = f"{self.table_path}/metadata"
        self.storage_type = self._detect_storage_type()
        
    def _detect_storage_type(self) -> str:
        """Detect the storage type from the path."""
        parsed = urlparse(self.table_path)
        # Use hdfs for GCS paths as well (company's HDFS connector supports GCS)
        if parsed.scheme == 'gs':
            return 'hdfs'  # Use hdfs dfs commands for GCS
        elif parsed.scheme in ['hdfs', 'viewfs']:
            return 'hdfs'
        elif parsed.scheme == 's3' or parsed.scheme == 's3a':
            return 's3'
        elif parsed.scheme == '' or parsed.scheme == 'file':
            return 'local'
        else:
            return 'unknown'
    
    def _read_file(self, file_path: str) -> str:
        """Read a file from the appropriate storage system."""
        if self.storage_type == 'hdfs':
            try:
                result = subprocess.run(['hdfs', 'dfs', '-cat', file_path], 
                                      capture_output=True, text=True, check=True)
                return result.stdout
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to read HDFS file {file_path}: {e.stderr}")
        elif self.storage_type == 's3':
            try:
                result = subprocess.run(['aws', 's3', 'cp', file_path, '-'], 
                                      capture_output=True, text=True, check=True)
                return result.stdout
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to read S3 file {file_path}: {e.stderr}")
        elif self.storage_type == 'local':
            with open(file_path, 'r') as f:
                return f.read()
        else:
            raise Exception(f"Unsupported storage type: {self.storage_type}")
    
    def _list_files(self, path: str) -> list:
        """List files in the metadata directory."""
        if self.storage_type == 'hdfs':
            try:
                result = subprocess.run(['hdfs', 'dfs', '-ls', path], 
                                      capture_output=True, text=True, check=True)
                lines = result.stdout.strip().split('\n')
                files = []
                for line in lines:
                    if line.startswith('Found'):
                        continue
                    parts = line.split()
                    if len(parts) >= 8:
                        files.append(parts[-1])
                return files
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to list HDFS files: {e.stderr}")
        elif self.storage_type == 's3':
            try:
                result = subprocess.run(['aws', 's3', 'ls', path], 
                                      capture_output=True, text=True, check=True)
                lines = result.stdout.strip().split('\n')
                files = []
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 4:
                        files.append(f"{path}/{parts[-1]}")
                return files
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to list S3 files: {e.stderr}")
        elif self.storage_type == 'local':
            return [os.path.join(path, f) for f in os.listdir(path)]
        else:
            raise Exception(f"Unsupported storage type: {self.storage_type}")
    
    def _find_latest_metadata_file(self) -> str:
        """Find the latest metadata JSON file using version-hint.text or metadata-log."""
        # First try to read version-hint.text which contains the latest version number
        version_hint_path = f"{self.metadata_path}/version-hint.text"
        
        try:
            version_hint = self._read_file(version_hint_path).strip()
            # version-hint.text contains just the version number
            if version_hint and version_hint.isdigit():
                return f"{self.metadata_path}/v{version_hint}.metadata.json"
        except Exception:
            # If version-hint.text doesn't exist or fails, fall back to other methods
            pass
        
        # If version-hint.text is not available, try to read v1.metadata.json
        # and follow the metadata-log chain
        try:
            v1_path = f"{self.metadata_path}/v1.metadata.json"
            metadata_content = self._read_file(v1_path)
            metadata = json.loads(metadata_content)
            
            # Check if there's a metadata-log with newer versions
            metadata_log = metadata.get('metadata-log', [])
            
            # Start with v1 as the latest
            latest_version = 1
            latest_path = v1_path
            
            # Try to find higher versions
            for version in range(2, 100):  # Reasonable upper limit
                try:
                    vn_path = f"{self.metadata_path}/v{version}.metadata.json"
                    content = self._read_file(vn_path)
                    # If we can read it, it exists
                    latest_version = version
                    latest_path = vn_path
                except Exception:
                    # This version doesn't exist, we found the latest
                    break
            
            return latest_path
            
        except Exception as e:
            # Final fallback: if storage supports listing, use the original method
            if self.storage_type in ['local', 'hdfs']:
                files = self._list_files(self.metadata_path)
                
                # Filter for metadata JSON files (v*.metadata.json)
                metadata_files = []
                for f in files:
                    if re.search(r'v\d+\.metadata\.json$', f):
                        metadata_files.append(f)
                
                if not metadata_files:
                    raise Exception("No metadata files found")
                
                # Sort by version number
                def extract_version(path):
                    match = re.search(r'v(\d+)\.metadata\.json$', path)
                    return int(match.group(1)) if match else 0
                
                metadata_files.sort(key=extract_version)
                return metadata_files[-1]
            else:
                raise Exception(f"Could not determine latest metadata file: {e}")
    
    def get_latest_snapshot_info(self) -> Dict[str, Any]:
        """
        Get information about the latest snapshot including operation details.
        
        Returns:
            Dictionary containing snapshot operation information
        """
        # Find and read the latest metadata file
        latest_metadata_file = self._find_latest_metadata_file()
        print(f"Reading metadata from: {latest_metadata_file}")
        
        metadata_content = self._read_file(latest_metadata_file)
        metadata = json.loads(metadata_content)
        
        # Get current snapshot ID
        current_snapshot_id = metadata.get('current-snapshot-id')
        if not current_snapshot_id:
            return {"error": "No current snapshot found"}
        
        # Find the current snapshot details
        current_snapshot = None
        for snapshot in metadata.get('snapshots', []):
            if snapshot['snapshot-id'] == current_snapshot_id:
                current_snapshot = snapshot
                break
        
        if not current_snapshot:
            return {"error": f"Snapshot {current_snapshot_id} not found"}
        
        # Extract operation information
        summary = current_snapshot.get('summary', {})
        timestamp_ms = current_snapshot.get('timestamp-ms', 0)
        
        # Calculate duration if available
        duration_info = self._calculate_duration(metadata, current_snapshot)
        
        # Format the results
        result = {
            "snapshot_id": current_snapshot_id,
            "operation": summary.get('operation', 'unknown'),
            "timestamp": datetime.fromtimestamp(timestamp_ms / 1000).isoformat() if timestamp_ms else "unknown",
            "files": {
                "inserted": {
                    "count": int(summary.get('added-data-files', 0)),
                    "size_bytes": int(summary.get('added-files-size', 0))
                },
                "updated": {
                    "count": int(summary.get('changed-data-files', 0)),
                    "size_bytes": int(summary.get('changed-files-size', 0))
                },
                "deleted": {
                    "count": int(summary.get('deleted-data-files', 0)),
                    "size_bytes": int(summary.get('deleted-files-size', 0))
                },
                "total": {
                    "count": int(summary.get('total-data-files', 0)),
                    "size_bytes": int(summary.get('total-files-size', 0))
                }
            },
            "records": {
                "inserted": int(summary.get('added-records', 0)),
                "updated": int(summary.get('changed-records', 0)),
                "deleted": int(summary.get('deleted-records', 0)),
                "total": int(summary.get('total-records', 0))
            },
            "partitions": {
                "written": int(summary.get('changed-partition-count', 0))
            },
            "delete_files": {
                "total": int(summary.get('total-delete-files', 0)),
                "position_deletes": int(summary.get('total-position-deletes', 0)),
                "equality_deletes": int(summary.get('total-equality-deletes', 0))
            },
            "duration": duration_info,
            "spark_app_id": summary.get('spark.app.id'),
            "sequence_number": current_snapshot.get('sequence-number')
        }
        
        return result
    
    def _calculate_duration(self, metadata: Dict, current_snapshot: Dict) -> Optional[str]:
        """Calculate the duration of the operation if possible."""
        # Try to find the previous snapshot to calculate duration
        current_timestamp = current_snapshot.get('timestamp-ms')
        if not current_timestamp:
            return None
        
        # Look for previous snapshot in snapshot-log
        snapshot_log = metadata.get('snapshot-log', [])
        current_idx = None
        
        for idx, entry in enumerate(snapshot_log):
            if entry.get('snapshot-id') == current_snapshot['snapshot-id']:
                current_idx = idx
                break
        
        if current_idx and current_idx > 0:
            prev_entry = snapshot_log[current_idx - 1]
            prev_timestamp = prev_entry.get('timestamp-ms')
            if prev_timestamp:
                duration_ms = current_timestamp - prev_timestamp
                duration = timedelta(milliseconds=duration_ms)
                
                # Format duration
                hours, remainder = divmod(duration.total_seconds(), 3600)
                minutes, seconds = divmod(remainder, 60)
                
                if hours > 0:
                    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                elif minutes > 0:
                    return f"{int(minutes)}m {int(seconds)}s"
                else:
                    return f"{seconds:.2f}s"
        
        return None
    
    def print_summary(self, info: Dict[str, Any]):
        """Print a formatted summary of the snapshot information."""
        if "error" in info:
            print(f"Error: {info['error']}")
            return
        
        print("\n" + "="*60)
        print("ICEBERG TABLE SNAPSHOT ANALYSIS")
        print("="*60)
        
        print(f"\nTable Location: {self.table_path}")
        print(f"Snapshot ID: {info['snapshot_id']}")
        print(f"Operation: {info['operation'].upper()}")
        print(f"Timestamp: {info['timestamp']}")
        if info['spark_app_id']:
            print(f"Spark App ID: {info['spark_app_id']}")
        print(f"Sequence Number: {info['sequence_number']}")
        
        print("\n" + "-"*40)
        print("FILES INFORMATION")
        print("-"*40)
        
        if info['files']['inserted']['count'] > 0:
            print(f"Files Inserted: {info['files']['inserted']['count']} "
                  f"({self._format_bytes(info['files']['inserted']['size_bytes'])})")
        
        if info['files']['updated']['count'] > 0:
            print(f"Files Updated: {info['files']['updated']['count']} "
                  f"({self._format_bytes(info['files']['updated']['size_bytes'])})")
        
        if info['files']['deleted']['count'] > 0:
            print(f"Files Deleted: {info['files']['deleted']['count']} "
                  f"({self._format_bytes(info['files']['deleted']['size_bytes'])})")
        
        print(f"Total Files: {info['files']['total']['count']} "
              f"({self._format_bytes(info['files']['total']['size_bytes'])})")
        
        print("\n" + "-"*40)
        print("RECORDS INFORMATION")
        print("-"*40)
        
        if info['records']['inserted'] > 0:
            print(f"Records Inserted: {info['records']['inserted']:,}")
        
        if info['records']['updated'] > 0:
            print(f"Records Updated: {info['records']['updated']:,}")
        
        if info['records']['deleted'] > 0:
            print(f"Records Deleted: {info['records']['deleted']:,}")
        
        print(f"Total Records: {info['records']['total']:,}")
        
        if info['partitions']['written'] > 0:
            print(f"\nPartitions Written: {info['partitions']['written']}")
        
        if info['delete_files']['total'] > 0:
            print(f"\nDelete Files: {info['delete_files']['total']}")
            if info['delete_files']['position_deletes'] > 0:
                print(f"  Position Deletes: {info['delete_files']['position_deletes']}")
            if info['delete_files']['equality_deletes'] > 0:
                print(f"  Equality Deletes: {info['delete_files']['equality_deletes']}")
        
        if info['duration']:
            print(f"\nOperation Duration: {info['duration']}")
        
        print("\n" + "="*60)
    
    def _format_bytes(self, size_bytes: int) -> str:
        """Format bytes into human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Iceberg table snapshots and extract operation information',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a GCS-based Iceberg table
  %(prog)s gs://bucket/path/to/iceberg/table/
  
  # Analyze an HDFS-based Iceberg table
  %(prog)s hdfs://namenode/path/to/iceberg/table/
  
  # Analyze a local Iceberg table
  %(prog)s /local/path/to/iceberg/table/
  
  # Output as JSON
  %(prog)s --json gs://bucket/path/to/iceberg/table/
        """
    )
    
    parser.add_argument('table_path', 
                       help='Path to the Iceberg table (e.g., gs://bucket/path/to/table/)')
    parser.add_argument('--json', action='store_true',
                       help='Output results as JSON instead of formatted text')
    
    args = parser.parse_args()
    
    try:
        analyzer = IcebergSnapshotAnalyzer(args.table_path)
        info = analyzer.get_latest_snapshot_info()
        
        if args.json:
            print(json.dumps(info, indent=2))
        else:
            analyzer.print_summary(info)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()