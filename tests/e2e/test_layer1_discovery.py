"""E2E Test: Layer 1 - Data Discovery against AGENTIC_PLATFORM.RAW"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.meta_agent.tools.data_scanner import DataScanner


def test_discovery():
    print("=" * 60)
    print("LAYER 1: DATA DISCOVERY TEST")
    print("Target: AGENTIC_PLATFORM.RAW")
    print("=" * 60)

    scanner = DataScanner(database="AGENTIC_PLATFORM")

    # Debug: Test raw query first
    print("\n[0] Debug: Testing raw query execution")
    try:
        results = scanner._execute("""
            SELECT ROW_COUNT, BYTES
            FROM AGENTIC_PLATFORM.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'ERP_SALES_ORDERS'
        """)
        print(f"    Raw results: {results}")
    except Exception as e:
        print(f"    Error: {e}")

    print("\n[1] Scanning table: AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS")
    try:
        asset1 = scanner.scan_table("AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS")
    except Exception as e:
        print(f"    Exception during scan: {e}")
        import traceback
        traceback.print_exc()
        return False
    if asset1:
        print(f"    ✓ Found: {asset1.name}")
        print(f"      Rows: {asset1.row_count}")
        print(f"      Columns: {asset1.column_count}")
        print(f"      Schema keys: {list(asset1.schema.keys())[:5]}...")
    else:
        print("    ✗ FAILED: Table not found")
        return False

    print("\n[2] Scanning table: AGENTIC_PLATFORM.RAW.MARKET_REPORTS")
    asset2 = scanner.scan_table("AGENTIC_PLATFORM.RAW.MARKET_REPORTS")
    if asset2:
        print(f"    ✓ Found: {asset2.name}")
        print(f"      Rows: {asset2.row_count}")
        print(f"      Columns: {asset2.column_count}")
        print(f"      Schema keys: {list(asset2.schema.keys())[:5]}...")
    else:
        print("    ✗ FAILED: Table not found")
        return False

    print("\n[3] Profiling discovered assets")
    assets = [asset1, asset2]
    profile = scanner.profile_assets(assets)
    print(f"    ✓ Total assets: {profile.total_assets}")
    print(f"      Structured: {profile.structured_count}")
    print(f"      Unstructured: {profile.unstructured_count}")
    print(f"      Total rows: {profile.total_rows}")
    print(f"      Has labeled data: {profile.has_labeled_data}")
    print(f"      Text content detected: {profile.text_content_detected}")
    print(f"      Potential targets: {profile.potential_target_columns}")
    print(f"      Potential features: {profile.potential_features[:10]}")

    print("\n" + "=" * 60)
    print("LAYER 1: PASSED ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_discovery()
    sys.exit(0 if success else 1)
