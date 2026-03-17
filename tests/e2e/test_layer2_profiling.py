"""E2E Test: Layer 2 - Schema Profiling against AGENTIC_PLATFORM.RAW"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.agents.discovery.schema_profiler import SchemaProfiler


def test_profiling():
    print("=" * 60)
    print("LAYER 2: SCHEMA PROFILING TEST")
    print("Target: AGENTIC_PLATFORM.RAW tables")
    print("=" * 60)

    profiler = SchemaProfiler(database="AGENTIC_PLATFORM")

    print("\n[1] Profiling ERP_SALES_ORDERS")
    try:
        profile1 = profiler.profile_table("AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS")
        print(f"    ✓ Table: {profile1.table_name}")
        print(f"      Rows: {profile1.row_count}")
        print(f"      Columns: {profile1.column_count}")
        print(f"      Size: {profile1.size_bytes} bytes")
        print(f"      PK candidates: {profile1.primary_key_candidates}")
        print(f"      Timestamp cols: {profile1.timestamp_columns}")
        print(f"      Numeric cols: {profile1.numeric_columns[:5]}...")
        print(f"      Text cols: {profile1.text_columns[:5]}...")
        
        print("\n    Column details (first 5):")
        for col in profile1.columns[:5]:
            print(f"      - {col.name}: {col.data_type} | semantic: {col.semantic_type} | nulls: {col.null_percentage}%")
            if col.sample_values:
                print(f"        samples: {col.sample_values[:3]}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[2] Profiling MARKET_REPORTS")
    try:
        profile2 = profiler.profile_table("AGENTIC_PLATFORM.RAW.MARKET_REPORTS")
        print(f"    ✓ Table: {profile2.table_name}")
        print(f"      Rows: {profile2.row_count}")
        print(f"      Text cols (for RAG): {profile2.text_columns}")
        
        # Check for content columns suitable for chunking
        content_cols = [c for c in profile2.columns if 'CONTENT' in c.name.upper() or 'TEXT' in c.name.upper() or 'SUMMARY' in c.name.upper()]
        if content_cols:
            print(f"      ✓ Found RAG-suitable columns: {[c.name for c in content_cols]}")
        else:
            print(f"      ⚠ No obvious content columns found for RAG")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "=" * 60)
    print("LAYER 2: PASSED ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_profiling()
    sys.exit(0 if success else 1)
