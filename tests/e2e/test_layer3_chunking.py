"""E2E Test: Layer 3 - Document Chunking from MARKET_REPORTS table"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.agents.preprocessing.document_chunker import DocumentChunker


def test_chunking():
    print("=" * 60)
    print("LAYER 3: DOCUMENT CHUNKING TEST")
    print("Source: AGENTIC_PLATFORM.RAW.MARKET_REPORTS")
    print("Target: AGENTIC_PLATFORM.DOCS.MARKET_CHUNKS")
    print("=" * 60)

    chunker = DocumentChunker(
        database="AGENTIC_PLATFORM",
        schema="DOCS",
        chunk_table="MARKET_CHUNKS",
        max_chunk_size=4000,
        chunk_overlap=200,
    )

    print("\n[1] Fetching documents from MARKET_REPORTS table")
    try:
        results = chunker._execute("""
            SELECT 
                REPORT_ID,
                REPORT_TITLE,
                REPORT_TYPE,
                CHEMICAL_NAME,
                REGION,
                REPORT_CONTENT
            FROM AGENTIC_PLATFORM.RAW.MARKET_REPORTS
            WHERE REPORT_CONTENT IS NOT NULL
            LIMIT 200
        """)
        print(f"    ✓ Found {len(results)} documents with content")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    if not results:
        print("    ✗ FAILED: No documents found")
        return False

    print("\n[2] Chunking documents")
    all_chunks = []
    for doc in results:
        report_id = doc.get('REPORT_ID')
        content = doc.get('REPORT_CONTENT', '')
        title = doc.get('REPORT_TITLE', 'Unknown')
        
        if not content:
            continue
            
        chunks = chunker.chunk(
            text=content,
            source_file=f"report_{report_id}",
            document_type="market_report"
        )
        
        # Enrich with metadata
        for chunk in chunks:
            chunk.metadata = {
                'report_id': report_id,
                'report_title': title,
                'chemical_name': doc.get('CHEMICAL_NAME'),
                'region': doc.get('REGION'),
                'report_type': doc.get('REPORT_TYPE'),
            }
        
        all_chunks.extend(chunks)
        
    print(f"    ✓ Created {len(all_chunks)} chunks from {len(results)} documents")
    
    if all_chunks:
        sample = all_chunks[0]
        print(f"    Sample chunk:")
        print(f"      ID: {sample.chunk_id}")
        print(f"      Section: {sample.section_header}")
        print(f"      Length: {len(sample.chunk_text)} chars")
        print(f"      Preview: {sample.chunk_text[:100]}...")

    print("\n[3] Loading chunks to Snowflake")
    try:
        loaded = chunker.load_chunks(all_chunks)
        print(f"    ✓ Loaded {loaded} chunks to AGENTIC_PLATFORM.DOCS.MARKET_CHUNKS")
    except Exception as e:
        print(f"    ✗ FAILED to load: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[4] Verifying chunk table")
    try:
        verify = chunker._execute("""
            SELECT COUNT(*) as cnt FROM AGENTIC_PLATFORM.DOCS.MARKET_CHUNKS
        """)
        count = verify[0]['CNT'] if verify else 0
        print(f"    ✓ Verified {count} chunks in table")
    except Exception as e:
        print(f"    ✗ Verification failed: {e}")
        return False

    print("\n" + "=" * 60)
    print("LAYER 3: PASSED ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_chunking()
    sys.exit(0 if success else 1)
