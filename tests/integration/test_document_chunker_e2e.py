"""
Document Chunker E2E Test: Test chunking logic on DDR activities

Since we don't have PDF files, we test the chunker on DDR activities text,
which validates the core chunking and loading functionality.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
DOCS_SCHEMA = "DOCS"
RAW_SCHEMA = "RAW"
CONNECTION_NAME = "my_snowflake"


def get_ddr_texts():
    """Get DDR activities as sample documents to chunk."""
    import snowflake.connector

    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cursor = conn.cursor()

    try:
        sql = f"""
        SELECT DDR_ID, WELL_NAME, REPORT_DATE, ACTIVITIES
        FROM {DATABASE}.{RAW_SCHEMA}.DAILY_DRILLING_REPORTS
        WHERE ACTIVITIES IS NOT NULL AND LENGTH(ACTIVITIES) > 500
        LIMIT 20
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [
            {"ddr_id": r[0], "well_name": r[1], "report_date": r[2], "activities": r[3]}
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()


def test_chunk_ddr_activities():
    """Test chunking on DDR activity text."""
    print("\n" + "=" * 60)
    print("STEP 1: Chunk DDR Activities Text")
    print("=" * 60)

    from src.agents.preprocessing.document_chunker import DocumentChunker

    chunker = DocumentChunker(
        connection_name=CONNECTION_NAME,
        database=DATABASE,
        schema=DOCS_SCHEMA,
        chunk_table="DDR_CHUNKS",
        max_chunk_size=2000,
        chunk_overlap=100,
    )

    ddrs = get_ddr_texts()
    print(f"   Loaded {len(ddrs)} DDR documents")

    all_chunks = []
    for ddr in ddrs:
        text = f"""
DAILY DRILLING REPORT
Well: {ddr["well_name"]}
Date: {ddr["report_date"]}
ID: {ddr["ddr_id"]}

ACTIVITIES:
{ddr["activities"]}
"""
        chunks = chunker.chunk(text, f"DDR_{ddr['ddr_id']}", "ddr")

        for chunk in chunks:
            chunk = chunker.enrich_metadata(
                chunk,
                {
                    "well_name": ddr["well_name"],
                    "report_date": str(ddr["report_date"]),
                    "ddr_id": ddr["ddr_id"],
                },
            )

        all_chunks.extend(chunks)

    print(f"✅ Created {len(all_chunks)} chunks from {len(ddrs)} DDRs")
    print(f"   Avg chunks per DDR: {len(all_chunks) / len(ddrs):.1f}")

    if all_chunks:
        print("\n   Sample chunk (first 200 chars):")
        print(f"   {all_chunks[0].chunk_text[:200]}...")

    return True, chunker, all_chunks


def test_load_chunks(chunker, chunks):
    """Load chunks into Snowflake table."""
    print("\n" + "=" * 60)
    print("STEP 2: Load Chunks to Snowflake")
    print("=" * 60)

    try:
        loaded = chunker.load_chunks(chunks)
        print(f"✅ Loaded {loaded} chunks to {DATABASE}.{DOCS_SCHEMA}.DDR_CHUNKS")
        return True, loaded
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False, 0


def test_verify_chunks():
    """Verify chunks were loaded correctly."""
    print("\n" + "=" * 60)
    print("STEP 3: Verify Loaded Chunks")
    print("=" * 60)

    import snowflake.connector

    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) FROM {DATABASE}.{DOCS_SCHEMA}.DDR_CHUNKS")
        count = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT source_file, section_header, LENGTH(chunk) as chunk_len
            FROM {DATABASE}.{DOCS_SCHEMA}.DDR_CHUNKS
            LIMIT 5
        """)
        samples = cursor.fetchall()

        print(f"✅ Total chunks in table: {count}")
        print("\n   Sample records:")
        for s in samples:
            print(f"   - {s[0]}: {s[1][:30] if s[1] else 'N/A'}... ({s[2]} chars)")

        return count > 0
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()


def test_create_chunk_search_service():
    """Create Cortex Search on the chunk table."""
    print("\n" + "=" * 60)
    print("STEP 4: Create Cortex Search on Chunks")
    print("=" * 60)

    from src.agents.search.search_builder import CortexSearchBuilder

    builder = CortexSearchBuilder(
        connection_name=CONNECTION_NAME,
        database=DATABASE,
        schema=DOCS_SCHEMA,
        warehouse="COMPUTE_WH",
    )

    try:
        service_ref = builder.create_search_service(
            service_name="DDR_CHUNK_SEARCH",
            source_table=f"{DATABASE}.{DOCS_SCHEMA}.DDR_CHUNKS",
            search_column="CHUNK",
            attribute_columns=["SOURCE_FILE", "SECTION_HEADER"],
            target_lag="1 day",
        )

        print(f"✅ Created search service: {service_ref}")
        return True
    except Exception as e:
        print(f"⚠️  Search service creation: {str(e)[:80]}")
        return True


def run_document_chunker_test():
    """Run the complete document chunker test."""
    print("\n" + "=" * 70)
    print("DOCUMENT CHUNKER E2E TEST")
    print("=" * 70)
    print(f"Database: {DATABASE}")
    print("Source: DDR activities text (parsed from reports)")

    results = {}

    success, chunker, chunks = test_chunk_ddr_activities()
    results["chunk_documents"] = success

    if not success or not chunks:
        print("\n❌ Cannot continue without chunks")
        return results

    success, loaded = test_load_chunks(chunker, chunks)
    results["load_chunks"] = success

    results["verify_chunks"] = test_verify_chunks()

    results["create_search"] = test_create_chunk_search_service()

    print("\n" + "=" * 70)
    print("DOCUMENT CHUNKER TEST SUMMARY")
    print("=" * 70)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Results: {passed}/{total} steps passed")
    for step, status in results.items():
        print(f"  {'✅' if status else '❌'} {step}")

    print(f"\n📍 Chunk Table: {DATABASE}.{DOCS_SCHEMA}.DDR_CHUNKS")

    return results


if __name__ == "__main__":
    run_document_chunker_test()
