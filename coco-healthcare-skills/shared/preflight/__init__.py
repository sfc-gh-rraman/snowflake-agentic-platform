"""Preflight checker for Health Sciences skills.

Reusable module that skills embed in their SKILL.md instructions to verify
external dependencies (Marketplace listings, Cortex Search services, tables,
stages) exist before proceeding — with graceful fallback guidance.

Usage in SKILL.md:
    Before starting, run the preflight check:
    ```sql
    -- Preflight: verify CKE PubMed listing is installed
    SELECT COUNT(*) FROM SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      '<db>.SHARED.<svc>', '{"query":"test","columns":["CONTENT"],"limit":1}'
    );
    ```
    If the query fails, see Setup Instructions below.
"""
