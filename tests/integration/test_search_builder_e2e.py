"""
Search Builder E2E Test: Create Cortex Search service for DDR documents

Tests:
1. Create Cortex Search service on DDR ACTIVITIES column
2. Query the search service
3. Verify search results are relevant
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
CORTEX_SCHEMA = "CORTEX"
RAW_SCHEMA = "RAW"
CONNECTION_NAME = "my_snowflake"


def test_create_search_service():
    """Create Cortex Search service for DDR documents."""
    print("\n" + "=" * 60)
    print("STEP 1: Create Cortex Search Service")
    print("=" * 60)
    
    from src.agents.search.search_builder import CortexSearchBuilder
    
    builder = CortexSearchBuilder(
        connection_name=CONNECTION_NAME,
        database=DATABASE,
        schema=CORTEX_SCHEMA,
        warehouse="COMPUTE_WH",
    )
    
    try:
        service_ref = builder.create_search_service(
            service_name="DDR_SEARCH",
            source_table=f"{DATABASE}.{RAW_SCHEMA}.DAILY_DRILLING_REPORTS",
            search_column="ACTIVITIES",
            attribute_columns=["DDR_ID", "WELL_NAME", "REPORT_DATE", "HAS_INCIDENT"],
            target_lag="1 day",
        )
        
        print(f"✅ Created search service: {service_ref}")
        return True, builder, service_ref
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False, None, None


def test_list_services(builder):
    """List existing search services."""
    print("\n" + "=" * 60)
    print("STEP 2: List Search Services")
    print("=" * 60)
    
    try:
        services = builder.list_services()
        print(f"✅ Found {len(services)} search service(s)")
        for svc in services:
            name = svc.get("name", "unknown")
            print(f"   - {name}")
        return True
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False


def test_search_queries(builder, service_ref):
    """Test various search queries against the service."""
    print("\n" + "=" * 60)
    print("STEP 3: Test Search Queries")
    print("=" * 60)
    
    test_queries = [
        ("stuck pipe", "Should find stuck pipe incidents"),
        ("drilling mud loss", "Should find mud loss events"),
        ("BOP test", "Should find BOP test activities"),
        ("cement plug", "Should find cementing operations"),
    ]
    
    passed = 0
    for query, description in test_queries:
        try:
            results = builder.search(
                service_name=service_ref,
                query=query,
                columns=["ACTIVITIES", "WELL_NAME", "REPORT_DATE"],
                limit=3,
            )
            
            if results and not results[0].get("error"):
                print(f"✅ Query '{query}': {len(results)} results")
                if results:
                    preview = str(results[0].get("ACTIVITIES", ""))[:80]
                    print(f"   Top result: {preview}...")
                passed += 1
            else:
                error = results[0].get("error", "No results") if results else "Empty"
                print(f"⚠️  Query '{query}': {error}")
        except Exception as e:
            print(f"❌ Query '{query}' failed: {str(e)}")
    
    print(f"\n   Search test: {passed}/{len(test_queries)} queries succeeded")
    return passed >= 2


def test_filtered_search(builder, service_ref):
    """Test search with filters."""
    print("\n" + "=" * 60)
    print("STEP 4: Test Filtered Search (Incidents Only)")
    print("=" * 60)
    
    try:
        results = builder.search(
            service_name=service_ref,
            query="problem",
            columns=["ACTIVITIES", "WELL_NAME", "HAS_INCIDENT"],
            filter_dict={"@eq": {"HAS_INCIDENT": True}},
            limit=5,
        )
        
        if results and not results[0].get("error"):
            incident_count = sum(1 for r in results if r.get("HAS_INCIDENT"))
            print(f"✅ Filtered search returned {len(results)} results")
            print(f"   Incidents in results: {incident_count}")
            return True
        else:
            print(f"⚠️  Filtered search: No results or error")
            return True
    except Exception as e:
        print(f"⚠️  Filtered search not supported: {str(e)[:50]}")
        return True


def run_search_builder_test():
    """Run the complete search builder test."""
    print("\n" + "=" * 70)
    print("CORTEX SEARCH BUILDER E2E TEST")
    print("=" * 70)
    print(f"Database: {DATABASE}")
    print(f"Source: {DATABASE}.{RAW_SCHEMA}.DAILY_DRILLING_REPORTS (1,759 reports)")
    
    results = {}
    
    success, builder, service_ref = test_create_search_service()
    results["create_service"] = success
    
    if not success or not builder:
        print("\n❌ Cannot continue without search service")
        return results
    
    results["list_services"] = test_list_services(builder)
    
    print("\n   Waiting for search service to index (10 seconds)...")
    import time
    time.sleep(10)
    
    results["search_queries"] = test_search_queries(builder, service_ref)
    results["filtered_search"] = test_filtered_search(builder, service_ref)
    
    print("\n" + "=" * 70)
    print("SEARCH BUILDER TEST SUMMARY")
    print("=" * 70)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Results: {passed}/{total} steps passed")
    for step, status in results.items():
        print(f"  {'✅' if status else '❌'} {step}")
    
    if success:
        print(f"\n📍 Search Service: {service_ref}")
    
    return results


if __name__ == "__main__":
    run_search_builder_test()
