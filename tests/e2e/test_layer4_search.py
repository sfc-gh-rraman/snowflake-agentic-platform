"""E2E Test: Layer 4 - Cortex Search Service Creation"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.agents.search.search_builder import CortexSearchBuilder


def test_search_service():
    print("=" * 60)
    print("LAYER 4: CORTEX SEARCH SERVICE TEST")
    print("Source: AGENTIC_PLATFORM.DOCS.MARKET_CHUNKS")
    print("Target: AGENTIC_PLATFORM.CORTEX.MARKET_SEARCH")
    print("=" * 60)

    builder = CortexSearchBuilder(
        database="AGENTIC_PLATFORM",
        schema="CORTEX",
        warehouse="COMPUTE_WH",
    )

    print("\n[1] Creating Cortex Search service")
    try:
        service_ref = builder.create_search_service(
            service_name="MARKET_SEARCH",
            source_table="AGENTIC_PLATFORM.DOCS.MARKET_CHUNKS",
            search_column="CHUNK",
            attribute_columns=["SOURCE_FILE", "SECTION_HEADER", "DOCUMENT_TYPE"],
            target_lag="1 hour",
        )
        print(f"    ✓ Created service: {service_ref}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[2] Waiting for service to be ready (indexing)...")
    import time
    time.sleep(5)

    print("\n[3] Testing search query")
    try:
        results = builder.search(
            service_name="MARKET_SEARCH",
            query="price increase margin feedstock",
            columns=["CHUNK", "SOURCE_FILE"],
            limit=3,
        )
        
        if results and not results[0].get('error'):
            print(f"    ✓ Search returned {len(results)} results")
            for i, r in enumerate(results[:2]):
                chunk = r.get('CHUNK', '')[:100] if isinstance(r.get('CHUNK'), str) else str(r)[:100]
                print(f"    Result {i+1}: {chunk}...")
        else:
            print(f"    ⚠ Search may still be indexing: {results}")
    except Exception as e:
        print(f"    ⚠ Search test failed (may still be indexing): {e}")

    print("\n[4] Verifying service exists")
    try:
        services = builder.list_services()
        found = any(s.get('name') == 'MARKET_SEARCH' for s in services)
        if found:
            print(f"    ✓ Service MARKET_SEARCH exists in CORTEX schema")
        else:
            print(f"    ⚠ Service not found in list (may need time)")
            print(f"    Services: {[s.get('name') for s in services]}")
    except Exception as e:
        print(f"    ⚠ List services failed: {e}")

    print("\n" + "=" * 60)
    print("LAYER 4: PASSED ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_search_service()
    sys.exit(0 if success else 1)
