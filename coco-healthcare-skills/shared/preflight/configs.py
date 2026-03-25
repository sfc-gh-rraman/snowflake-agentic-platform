#!/usr/bin/env python3
"""Preflight configurations for Health Sciences skills.

Pre-built checker instances for common dependencies:
- CKE PubMed (Marketplace listing)
- CKE Clinical Trials (Marketplace listing)
- DICOM Data Model Knowledge (Cortex Search + table)

Usage:
    from shared.preflight.configs import create_cke_pubmed_checker
    checker = create_cke_pubmed_checker(conn)
    results = checker.run()
    checker.print_report(results)
"""

from .checker import PreflightChecker


def create_cke_pubmed_checker(conn=None):
    checker = PreflightChecker(conn)
    checker.add_marketplace_listing(
        name="CKE PubMed — Snowflake Marketplace Listing",
        db_name="PUBMED_ABSTRACTS_EMBEDDINGS",
        test_table="SHARED.PUBMED_SEARCH_CORPUS",
        listing_url="https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQW",
        setup="Accept the listing and grant access to your SYSADMIN role",
        fallback="Skill will work without PubMed search — no biomedical literature enrichment available.",
        required=False,
    )
    return checker


def create_cke_clinical_trials_checker(conn=None):
    checker = PreflightChecker(conn)
    checker.add_marketplace_listing(
        name="CKE Clinical Trials — Snowflake Marketplace Listing",
        db_name="CLINICAL_TRIALS_EMBEDDINGS",
        test_table="SHARED.CLINICAL_TRIALS_SEARCH_CORPUS",
        listing_url="https://app.snowflake.com/marketplace/listing/GZSTZ67BY9ORD",
        setup="Accept the listing and grant access to your SYSADMIN role",
        fallback="Skill will work without ClinicalTrials.gov search — no trial benchmarking available.",
        required=False,
    )
    return checker


def create_dicom_model_knowledge_checker(conn=None):
    checker = PreflightChecker(conn)
    checker.add_table(
        name="DICOM Data Model Knowledge — Reference Table",
        fqn="UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_DOCS",
        setup="Run: python scripts/setup_dicom_model_knowledge_repo.sql\n"
              "       This creates the DATA_MODEL_KNOWLEDGE schema, loads reference docs,\n"
              "       and creates the Cortex Search Service.",
        fallback="Skill will use hardcoded DICOM schema definitions from SKILL.md references.",
        required=False,
    )
    checker.add_cortex_search(
        name="DICOM Model Search Service — Cortex Search",
        svc_fqn="UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC",
        setup="Created automatically by setup_dicom_model_knowledge_repo.sql",
        fallback="Skill will use hardcoded schema definitions instead of dynamic semantic search.",
        required=False,
    )
    return checker


def create_full_preflight(conn=None):
    checker = PreflightChecker(conn)

    checker.add_marketplace_listing(
        name="CKE PubMed",
        db_name="PUBMED_ABSTRACTS_EMBEDDINGS",
        test_table="SHARED.PUBMED_SEARCH_CORPUS",
        listing_url="https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQW",
        setup="Accept listing, grant SYSADMIN access",
        fallback="No biomedical literature enrichment",
        required=False,
    )

    checker.add_marketplace_listing(
        name="CKE Clinical Trials",
        db_name="CLINICAL_TRIALS_EMBEDDINGS",
        test_table="SHARED.CLINICAL_TRIALS_SEARCH_CORPUS",
        listing_url="https://app.snowflake.com/marketplace/listing/GZSTZ67BY9ORD",
        setup="Accept listing, grant SYSADMIN access",
        fallback="No trial benchmarking",
        required=False,
    )

    checker.add_table(
        name="DICOM Data Model Knowledge",
        fqn="UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_DOCS",
        setup="Run scripts/setup_dicom_model_knowledge_repo.sql",
        fallback="Uses hardcoded DICOM schemas",
        required=False,
    )

    checker.add_cortex_search(
        name="DICOM Model Search Service",
        svc_fqn="UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.DICOM_MODEL_SEARCH_SVC",
        setup="Created by setup script",
        fallback="Uses hardcoded schema definitions",
        required=False,
    )

    return checker
