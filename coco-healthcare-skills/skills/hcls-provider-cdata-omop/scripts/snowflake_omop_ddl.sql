-- OMOP CDM v5.4 DDL for Snowflake
-- Based on OHDSI CommonDataModel

-- ============================================
-- STANDARDIZED VOCABULARY TABLES
-- ============================================

CREATE TABLE CONCEPT (
    concept_id INTEGER NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    domain_id VARCHAR(20) NOT NULL,
    vocabulary_id VARCHAR(20) NOT NULL,
    concept_class_id VARCHAR(20) NOT NULL,
    standard_concept VARCHAR(1),
    concept_code VARCHAR(50) NOT NULL,
    valid_start_date DATE NOT NULL,
    valid_end_date DATE NOT NULL,
    invalid_reason VARCHAR(1),
    PRIMARY KEY (concept_id)
);

CREATE TABLE VOCABULARY (
    vocabulary_id VARCHAR(20) NOT NULL,
    vocabulary_name VARCHAR(255) NOT NULL,
    vocabulary_reference VARCHAR(255),
    vocabulary_version VARCHAR(255),
    vocabulary_concept_id INTEGER NOT NULL,
    PRIMARY KEY (vocabulary_id)
);

CREATE TABLE DOMAIN (
    domain_id VARCHAR(20) NOT NULL,
    domain_name VARCHAR(255) NOT NULL,
    domain_concept_id INTEGER NOT NULL,
    PRIMARY KEY (domain_id)
);

CREATE TABLE CONCEPT_CLASS (
    concept_class_id VARCHAR(20) NOT NULL,
    concept_class_name VARCHAR(255) NOT NULL,
    concept_class_concept_id INTEGER NOT NULL,
    PRIMARY KEY (concept_class_id)
);

CREATE TABLE CONCEPT_RELATIONSHIP (
    concept_id_1 INTEGER NOT NULL,
    concept_id_2 INTEGER NOT NULL,
    relationship_id VARCHAR(20) NOT NULL,
    valid_start_date DATE NOT NULL,
    valid_end_date DATE NOT NULL,
    invalid_reason VARCHAR(1)
);

CREATE TABLE RELATIONSHIP (
    relationship_id VARCHAR(20) NOT NULL,
    relationship_name VARCHAR(255) NOT NULL,
    is_hierarchical VARCHAR(1) NOT NULL,
    defines_ancestry VARCHAR(1) NOT NULL,
    reverse_relationship_id VARCHAR(20) NOT NULL,
    relationship_concept_id INTEGER NOT NULL,
    PRIMARY KEY (relationship_id)
);

CREATE TABLE CONCEPT_SYNONYM (
    concept_id INTEGER NOT NULL,
    concept_synonym_name VARCHAR(1000) NOT NULL,
    language_concept_id INTEGER NOT NULL
);

CREATE TABLE CONCEPT_ANCESTOR (
    ancestor_concept_id INTEGER NOT NULL,
    descendant_concept_id INTEGER NOT NULL,
    min_levels_of_separation INTEGER NOT NULL,
    max_levels_of_separation INTEGER NOT NULL
);

CREATE TABLE DRUG_STRENGTH (
    drug_concept_id INTEGER NOT NULL,
    ingredient_concept_id INTEGER NOT NULL,
    amount_value FLOAT,
    amount_unit_concept_id INTEGER,
    numerator_value FLOAT,
    numerator_unit_concept_id INTEGER,
    denominator_value FLOAT,
    denominator_unit_concept_id INTEGER,
    box_size INTEGER,
    valid_start_date DATE NOT NULL,
    valid_end_date DATE NOT NULL,
    invalid_reason VARCHAR(1)
);

-- ============================================
-- CLINICAL DATA TABLES
-- ============================================

CREATE TABLE PERSON (
    person_id INTEGER NOT NULL,
    gender_concept_id INTEGER NOT NULL,
    year_of_birth INTEGER NOT NULL,
    month_of_birth INTEGER,
    day_of_birth INTEGER,
    birth_datetime TIMESTAMP,
    race_concept_id INTEGER NOT NULL,
    ethnicity_concept_id INTEGER NOT NULL,
    location_id INTEGER,
    provider_id INTEGER,
    care_site_id INTEGER,
    person_source_value VARCHAR(50),
    gender_source_value VARCHAR(50),
    gender_source_concept_id INTEGER,
    race_source_value VARCHAR(50),
    race_source_concept_id INTEGER,
    ethnicity_source_value VARCHAR(50),
    ethnicity_source_concept_id INTEGER,
    PRIMARY KEY (person_id)
);

CREATE TABLE OBSERVATION_PERIOD (
    observation_period_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    observation_period_start_date DATE NOT NULL,
    observation_period_end_date DATE NOT NULL,
    period_type_concept_id INTEGER NOT NULL,
    PRIMARY KEY (observation_period_id)
);

CREATE TABLE VISIT_OCCURRENCE (
    visit_occurrence_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    visit_concept_id INTEGER NOT NULL,
    visit_start_date DATE NOT NULL,
    visit_start_datetime TIMESTAMP,
    visit_end_date DATE NOT NULL,
    visit_end_datetime TIMESTAMP,
    visit_type_concept_id INTEGER NOT NULL,
    provider_id INTEGER,
    care_site_id INTEGER,
    visit_source_value VARCHAR(50),
    visit_source_concept_id INTEGER,
    admitted_from_concept_id INTEGER,
    admitted_from_source_value VARCHAR(50),
    discharged_to_concept_id INTEGER,
    discharged_to_source_value VARCHAR(50),
    preceding_visit_occurrence_id INTEGER,
    PRIMARY KEY (visit_occurrence_id)
);

CREATE TABLE VISIT_DETAIL (
    visit_detail_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    visit_detail_concept_id INTEGER NOT NULL,
    visit_detail_start_date DATE NOT NULL,
    visit_detail_start_datetime TIMESTAMP,
    visit_detail_end_date DATE NOT NULL,
    visit_detail_end_datetime TIMESTAMP,
    visit_detail_type_concept_id INTEGER NOT NULL,
    provider_id INTEGER,
    care_site_id INTEGER,
    visit_detail_source_value VARCHAR(50),
    visit_detail_source_concept_id INTEGER,
    admitted_from_concept_id INTEGER,
    admitted_from_source_value VARCHAR(50),
    discharged_to_concept_id INTEGER,
    discharged_to_source_value VARCHAR(50),
    preceding_visit_detail_id INTEGER,
    parent_visit_detail_id INTEGER,
    visit_occurrence_id INTEGER NOT NULL,
    PRIMARY KEY (visit_detail_id)
);

CREATE TABLE CONDITION_OCCURRENCE (
    condition_occurrence_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    condition_concept_id INTEGER NOT NULL,
    condition_start_date DATE NOT NULL,
    condition_start_datetime TIMESTAMP,
    condition_end_date DATE,
    condition_end_datetime TIMESTAMP,
    condition_type_concept_id INTEGER NOT NULL,
    condition_status_concept_id INTEGER,
    stop_reason VARCHAR(20),
    provider_id INTEGER,
    visit_occurrence_id INTEGER,
    visit_detail_id INTEGER,
    condition_source_value VARCHAR(50),
    condition_source_concept_id INTEGER,
    condition_status_source_value VARCHAR(50),
    PRIMARY KEY (condition_occurrence_id)
);

CREATE TABLE DRUG_EXPOSURE (
    drug_exposure_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    drug_concept_id INTEGER NOT NULL,
    drug_exposure_start_date DATE NOT NULL,
    drug_exposure_start_datetime TIMESTAMP,
    drug_exposure_end_date DATE NOT NULL,
    drug_exposure_end_datetime TIMESTAMP,
    verbatim_end_date DATE,
    drug_type_concept_id INTEGER NOT NULL,
    stop_reason VARCHAR(20),
    refills INTEGER,
    quantity FLOAT,
    days_supply INTEGER,
    sig TEXT,
    route_concept_id INTEGER,
    lot_number VARCHAR(50),
    provider_id INTEGER,
    visit_occurrence_id INTEGER,
    visit_detail_id INTEGER,
    drug_source_value VARCHAR(50),
    drug_source_concept_id INTEGER,
    route_source_value VARCHAR(50),
    dose_unit_source_value VARCHAR(50),
    PRIMARY KEY (drug_exposure_id)
);

CREATE TABLE PROCEDURE_OCCURRENCE (
    procedure_occurrence_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    procedure_concept_id INTEGER NOT NULL,
    procedure_date DATE NOT NULL,
    procedure_datetime TIMESTAMP,
    procedure_end_date DATE,
    procedure_end_datetime TIMESTAMP,
    procedure_type_concept_id INTEGER NOT NULL,
    modifier_concept_id INTEGER,
    quantity INTEGER,
    provider_id INTEGER,
    visit_occurrence_id INTEGER,
    visit_detail_id INTEGER,
    procedure_source_value VARCHAR(50),
    procedure_source_concept_id INTEGER,
    modifier_source_value VARCHAR(50),
    PRIMARY KEY (procedure_occurrence_id)
);

CREATE TABLE MEASUREMENT (
    measurement_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    measurement_concept_id INTEGER NOT NULL,
    measurement_date DATE NOT NULL,
    measurement_datetime TIMESTAMP,
    measurement_time VARCHAR(10),
    measurement_type_concept_id INTEGER NOT NULL,
    operator_concept_id INTEGER,
    value_as_number FLOAT,
    value_as_concept_id INTEGER,
    unit_concept_id INTEGER,
    range_low FLOAT,
    range_high FLOAT,
    provider_id INTEGER,
    visit_occurrence_id INTEGER,
    visit_detail_id INTEGER,
    measurement_source_value VARCHAR(50),
    measurement_source_concept_id INTEGER,
    unit_source_value VARCHAR(50),
    unit_source_concept_id INTEGER,
    value_source_value VARCHAR(50),
    measurement_event_id INTEGER,
    meas_event_field_concept_id INTEGER,
    PRIMARY KEY (measurement_id)
);

CREATE TABLE OBSERVATION (
    observation_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    observation_concept_id INTEGER NOT NULL,
    observation_date DATE NOT NULL,
    observation_datetime TIMESTAMP,
    observation_type_concept_id INTEGER NOT NULL,
    value_as_number FLOAT,
    value_as_string VARCHAR(60),
    value_as_concept_id INTEGER,
    qualifier_concept_id INTEGER,
    unit_concept_id INTEGER,
    provider_id INTEGER,
    visit_occurrence_id INTEGER,
    visit_detail_id INTEGER,
    observation_source_value VARCHAR(50),
    observation_source_concept_id INTEGER,
    unit_source_value VARCHAR(50),
    qualifier_source_value VARCHAR(50),
    value_source_value VARCHAR(50),
    observation_event_id INTEGER,
    obs_event_field_concept_id INTEGER,
    PRIMARY KEY (observation_id)
);

CREATE TABLE DEATH (
    person_id INTEGER NOT NULL,
    death_date DATE NOT NULL,
    death_datetime TIMESTAMP,
    death_type_concept_id INTEGER,
    cause_concept_id INTEGER,
    cause_source_value VARCHAR(50),
    cause_source_concept_id INTEGER,
    PRIMARY KEY (person_id)
);

-- ============================================
-- HEALTH SYSTEM DATA TABLES
-- ============================================

CREATE TABLE LOCATION (
    location_id INTEGER NOT NULL,
    address_1 VARCHAR(50),
    address_2 VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(2),
    zip VARCHAR(9),
    county VARCHAR(20),
    location_source_value VARCHAR(50),
    country_concept_id INTEGER,
    country_source_value VARCHAR(80),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (location_id)
);

CREATE TABLE CARE_SITE (
    care_site_id INTEGER NOT NULL,
    care_site_name VARCHAR(255),
    place_of_service_concept_id INTEGER,
    location_id INTEGER,
    care_site_source_value VARCHAR(50),
    place_of_service_source_value VARCHAR(50),
    PRIMARY KEY (care_site_id)
);

CREATE TABLE PROVIDER (
    provider_id INTEGER NOT NULL,
    provider_name VARCHAR(255),
    npi VARCHAR(20),
    dea VARCHAR(20),
    specialty_concept_id INTEGER,
    care_site_id INTEGER,
    year_of_birth INTEGER,
    gender_concept_id INTEGER,
    provider_source_value VARCHAR(50),
    specialty_source_value VARCHAR(50),
    specialty_source_concept_id INTEGER,
    gender_source_value VARCHAR(50),
    gender_source_concept_id INTEGER,
    PRIMARY KEY (provider_id)
);

-- ============================================
-- DERIVED ELEMENTS
-- ============================================

CREATE TABLE DRUG_ERA (
    drug_era_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    drug_concept_id INTEGER NOT NULL,
    drug_era_start_date DATE NOT NULL,
    drug_era_end_date DATE NOT NULL,
    drug_exposure_count INTEGER,
    gap_days INTEGER,
    PRIMARY KEY (drug_era_id)
);

CREATE TABLE CONDITION_ERA (
    condition_era_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    condition_concept_id INTEGER NOT NULL,
    condition_era_start_date DATE NOT NULL,
    condition_era_end_date DATE NOT NULL,
    condition_occurrence_count INTEGER,
    PRIMARY KEY (condition_era_id)
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX idx_concept_code ON CONCEPT (concept_code);
CREATE INDEX idx_concept_vocab ON CONCEPT (vocabulary_id);
CREATE INDEX idx_concept_domain ON CONCEPT (domain_id);
CREATE INDEX idx_cr_c1 ON CONCEPT_RELATIONSHIP (concept_id_1);
CREATE INDEX idx_cr_c2 ON CONCEPT_RELATIONSHIP (concept_id_2);
CREATE INDEX idx_person_id ON PERSON (person_id);
CREATE INDEX idx_visit_person ON VISIT_OCCURRENCE (person_id);
CREATE INDEX idx_condition_person ON CONDITION_OCCURRENCE (person_id);
CREATE INDEX idx_condition_concept ON CONDITION_OCCURRENCE (condition_concept_id);
CREATE INDEX idx_drug_person ON DRUG_EXPOSURE (person_id);
CREATE INDEX idx_drug_concept ON DRUG_EXPOSURE (drug_concept_id);
CREATE INDEX idx_measurement_person ON MEASUREMENT (person_id);
CREATE INDEX idx_measurement_concept ON MEASUREMENT (measurement_concept_id);
CREATE INDEX idx_procedure_person ON PROCEDURE_OCCURRENCE (person_id);
CREATE INDEX idx_observation_person ON OBSERVATION (person_id);
