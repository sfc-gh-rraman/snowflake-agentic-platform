---
name: hcls-provider-imaging-dicom-parser
description: Parse DICOM medical image metadata and create standardized data models on Snowflake. Use when extracting DICOM tags, building radiology metadata tables, loading imaging study data, or analyzing medical imaging metadata. Triggers include DICOM, radiology, medical imaging, CT scan, MRI, X-ray, PACS, imaging metadata, DICOM tags.
---

# DICOM Metadata Parser

Parse DICOM (Digital Imaging and Communications in Medicine) file metadata and load into Snowflake for analytics.

## When to Use This Skill

- Extracting metadata from DICOM files (.dcm)
- Building radiology/imaging metadata data models
- Loading imaging study information into Snowflake
- Analyzing medical imaging workflow data
- Creating PACS (Picture Archiving and Communication System) analytics
- Preparing imaging data for ML/AI embeddings

## Quick Start

```python
# Parse a single DICOM file
python scripts/parse_dicom.py image.dcm --output-dir ./output

# Parse directory of DICOM files
python scripts/parse_dicom.py ./dicom_folder --recursive --output-dir ./output

# Load directly to Snowflake
python scripts/parse_dicom.py ./dicom_folder --snowflake --database IMAGING --schema DICOM_RAW
```

## Comprehensive DICOM Data Model

This model follows the DICOM hierarchy: Patient -> Study -> Series -> Instance -> Frame, with supporting entities for equipment, procedure context, dose tracking, and derived objects.

### Entity Relationship Diagram

```
DicomPatient (1) --> (many) DicomStudy
DicomStudy (1) --> (many) DicomSeries
DicomSeries (1) --> (many) DicomInstance
DicomInstance (1) --> (0..many) DicomFrame
DicomSeries (1) --> (0..many) DicomEquipment
DicomStudy (1) --> (0..many) DicomProcedureStep
DicomSeries/Study (1) --> (0..many) DicomDoseSummary
DicomInstance (1) --> (0..1) DicomImagePixel
DicomInstance/DicomFrame (1) --> (0..1) DicomImagePlane
DicomInstance (1) --> (0..many) DicomElement
DicomElement (SQ) (1) --> (0..many) DicomSequenceItem
DicomInstance (1) --> (0..many) DicomFileLocation
DicomInstance (SEG) (1) --> (0..many) DicomSegmentationMetadata
DicomInstance (SR) (1) --> (0..1) DicomStructuredReportHeader
DicomInstance (1) --> (0..many) ImageEmbedding
```

### Core Hierarchy Entities

#### 1. DICOM_PATIENT
Logical patient entity as represented in DICOM headers (not an enterprise MPI).

```sql
CREATE OR REPLACE TABLE dicom_patient (
    patient_key INTEGER AUTOINCREMENT PRIMARY KEY,
    source_system VARCHAR,
    patient_id VARCHAR NOT NULL,
    issuer_of_patient_id VARCHAR,
    patient_name VARCHAR,
    patient_sex VARCHAR(16),
    patient_birth_date DATE,
    patient_age VARCHAR(16),
    other_patient_ids ARRAY,
    other_patient_names ARRAY,
    comments VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (patient_id, issuer_of_patient_id)
);
```

#### 2. DICOM_STUDY
Study-level context (order/visit/exam episode).

```sql
CREATE OR REPLACE TABLE dicom_study (
    study_key INTEGER AUTOINCREMENT PRIMARY KEY,
    patient_key INTEGER REFERENCES dicom_patient(patient_key),
    study_instance_uid VARCHAR NOT NULL UNIQUE,
    accession_number VARCHAR,
    study_id VARCHAR,
    study_datetime TIMESTAMP_NTZ,
    study_date DATE,
    study_time TIME,
    study_description VARCHAR,
    referring_physician VARCHAR,
    admitting_diagnosis VARCHAR,
    study_instance_uid_root VARCHAR,
    number_of_series INTEGER,
    number_of_instances INTEGER,
    modalities_in_study ARRAY,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 3. DICOM_SERIES
Group of instances with common acquisition context.

```sql
CREATE OR REPLACE TABLE dicom_series (
    series_key INTEGER AUTOINCREMENT PRIMARY KEY,
    study_key INTEGER REFERENCES dicom_study(study_key),
    series_instance_uid VARCHAR NOT NULL UNIQUE,
    series_number INTEGER,
    modality VARCHAR(16) NOT NULL,
    body_part_examined VARCHAR,
    laterality VARCHAR(16),
    series_description VARCHAR,
    frame_of_reference_uid VARCHAR,
    patient_position VARCHAR(16),
    performed_station_name VARCHAR,
    performed_location VARCHAR,
    series_date DATE,
    series_time TIME,
    protocol_name VARCHAR,
    number_of_instances INTEGER,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 4. DICOM_INSTANCE
Individual DICOM SOP instance (image, SR, SEG, RT, etc.).

```sql
CREATE OR REPLACE TABLE dicom_instance (
    instance_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    sop_instance_uid VARCHAR NOT NULL UNIQUE,
    sop_class_uid VARCHAR NOT NULL,
    instance_number INTEGER,
    image_type ARRAY,
    acquisition_datetime TIMESTAMP_NTZ,
    content_datetime TIMESTAMP_NTZ,
    acquisition_date DATE,
    acquisition_time TIME,
    content_date DATE,
    content_time TIME,
    number_of_frames INTEGER DEFAULT 1,
    specific_character_set VARCHAR,
    burned_in_annotation VARCHAR,
    presentation_intent VARCHAR,
    file_path VARCHAR,
    file_size_bytes INTEGER,
    transfer_syntax_uid VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 5. DICOM_FRAME (for multi-frame objects)
Per-frame metadata for enhanced CT/MR, SEG, etc.

```sql
CREATE OR REPLACE TABLE dicom_frame (
    frame_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_number INTEGER NOT NULL,
    frame_content_datetime TIMESTAMP_NTZ,
    image_position_patient ARRAY,
    image_orientation_patient ARRAY,
    slice_location FLOAT,
    temporal_position_index INTEGER,
    cardiac_cycle_position FLOAT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (instance_key, frame_number)
);
```

### Technical and Acquisition Context

#### 6. DICOM_EQUIPMENT
Acquisition device/scanner details.

```sql
CREATE OR REPLACE TABLE dicom_equipment (
    equipment_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    manufacturer VARCHAR,
    manufacturer_model_name VARCHAR,
    device_serial_number VARCHAR,
    software_versions VARCHAR,
    institution_name VARCHAR,
    institution_address VARCHAR,
    station_name VARCHAR,
    institutional_dept_name VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 7. DICOM_IMAGE_PIXEL
Pixel grid and encoding characteristics (not pixel values).

```sql
CREATE OR REPLACE TABLE dicom_image_pixel (
    image_pixel_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    image_rows INTEGER,
    image_columns INTEGER,
    number_of_frames INTEGER,
    samples_per_pixel INTEGER,
    photometric_interpretation VARCHAR,
    bits_allocated INTEGER,
    bits_stored INTEGER,
    high_bit INTEGER,
    pixel_representation INTEGER,
    planar_configuration INTEGER,
    rescale_intercept FLOAT,
    rescale_slope FLOAT,
    window_center ARRAY,
    window_width ARRAY,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 8. DICOM_IMAGE_PLANE
Spatial resolution and positioning.

```sql
CREATE OR REPLACE TABLE dicom_image_plane (
    image_plane_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_key INTEGER REFERENCES dicom_frame(frame_key),
    pixel_spacing ARRAY,
    slice_thickness FLOAT,
    image_position_patient ARRAY,
    image_orientation_patient ARRAY,
    spacing_between_slices FLOAT,
    position_reference_indicator VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Workflow and Procedure Context

#### 9. DICOM_PROCEDURE_STEP
Mapping to requested/performed procedures and codes.

```sql
CREATE OR REPLACE TABLE dicom_procedure_step (
    procedure_key INTEGER AUTOINCREMENT PRIMARY KEY,
    study_key INTEGER REFERENCES dicom_study(study_key),
    requested_procedure_id VARCHAR,
    requested_procedure_description VARCHAR,
    requested_procedure_code_seq VARIANT,
    performed_procedure_step_id VARCHAR,
    performed_procedure_description VARCHAR,
    performed_procedure_type VARCHAR,
    performed_procedure_code_seq VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Dose and Exposure (CT/Radiography)

#### 10. DICOM_DOSE_SUMMARY
Summarized exposure parameters for analytics.

```sql
CREATE OR REPLACE TABLE dicom_dose_summary (
    dose_key INTEGER AUTOINCREMENT PRIMARY KEY,
    series_key INTEGER REFERENCES dicom_series(series_key),
    study_key INTEGER REFERENCES dicom_study(study_key),
    ctdi_vol FLOAT,
    dose_length_product FLOAT,
    exposure_time FLOAT,
    kvp FLOAT,
    xray_tube_current FLOAT,
    exposure FLOAT,
    acquisition_protocol VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Derived Object Metadata (SEG, SR)

#### 11. DICOM_SEGMENTATION_METADATA
Metadata describing segmentation objects (SEG SOP).

```sql
CREATE OR REPLACE TABLE dicom_segmentation_metadata (
    segmentation_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    referenced_series_key INTEGER REFERENCES dicom_series(series_key),
    segment_number INTEGER,
    segment_label VARCHAR,
    segment_description VARCHAR,
    segmentation_type VARCHAR,
    segmentation_fractional_type VARCHAR,
    recommended_display_grayscale VARIANT,
    anatomic_region_code_seq VARIANT,
    property_category_code_seq VARIANT,
    property_type_code_seq VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 12. DICOM_STRUCTURED_REPORT_HEADER
High-level metadata for Structured Reports (SR SOP).

```sql
CREATE OR REPLACE TABLE dicom_structured_report_header (
    sr_header_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    completion_flag VARCHAR,
    verification_flag VARCHAR,
    document_title VARCHAR,
    coding_scheme_identification VARIANT,
    referenced_instance_keys ARRAY,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Physical Storage/Location

#### 13. DICOM_FILE_LOCATION
Where each DICOM object is stored and how to access it.

```sql
CREATE OR REPLACE TABLE dicom_file_location (
    location_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    storage_uri VARCHAR,
    storage_provider VARCHAR,
    storage_container VARCHAR,
    object_key VARCHAR,
    transfer_syntax_uid VARCHAR,
    checksum VARCHAR,
    ingestion_source VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Generic Element & Sequence Store

#### 14. DICOM_ELEMENT
Generic store for any DICOM data element (especially long-tail and private tags).

```sql
CREATE OR REPLACE TABLE dicom_element (
    element_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_number INTEGER,
    tag_group INTEGER,
    tag_element INTEGER,
    tag_hex VARCHAR(8),
    name VARCHAR,
    vr VARCHAR(4),
    vm INTEGER,
    value_string VARCHAR,
    value_number FLOAT,
    value_datetime TIMESTAMP_NTZ,
    value_binary_ref VARCHAR,
    is_private BOOLEAN,
    private_creator VARCHAR,
    sequence_item_key INTEGER,
    sequence_path VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 15. DICOM_SEQUENCE_ITEM
Represents a single item within a sequence (SQ) element.

```sql
CREATE OR REPLACE TABLE dicom_sequence_item (
    sequence_item_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    parent_element_key INTEGER REFERENCES dicom_element(element_key),
    item_index INTEGER,
    sequence_path VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Image Embeddings (for ML/AI)

#### 16. IMAGE_EMBEDDING
Vector representations of images for similarity search and ML.

```sql
CREATE OR REPLACE TABLE image_embedding (
    embedding_key INTEGER AUTOINCREMENT PRIMARY KEY,
    instance_key INTEGER REFERENCES dicom_instance(instance_key),
    frame_key INTEGER REFERENCES dicom_frame(frame_key),
    segmentation_key INTEGER REFERENCES dicom_segmentation_metadata(segmentation_key),
    embedding_vector ARRAY,
    model_key INTEGER REFERENCES embedding_model(model_key),
    representation_scope VARCHAR,
    representation_version VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_image_uri VARCHAR
);
```

#### 17. EMBEDDING_MODEL
Catalog of embedding models used to generate ImageEmbedding vectors.

```sql
CREATE OR REPLACE TABLE embedding_model (
    model_key INTEGER AUTOINCREMENT PRIMARY KEY,
    model_name VARCHAR NOT NULL,
    model_version VARCHAR,
    modality_scope ARRAY,
    task_scope ARRAY,
    dimensionality INTEGER,
    training_data_summary VARCHAR,
    preprocessing_spec VARIANT,
    postprocessing_notes VARCHAR,
    owning_team VARCHAR,
    regulatory_notes VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 18. EMBEDDING_EVALUATION
Track quality metrics for a model on representative datasets.

```sql
CREATE OR REPLACE TABLE embedding_evaluation (
    evaluation_key INTEGER AUTOINCREMENT PRIMARY KEY,
    model_key INTEGER REFERENCES embedding_model(model_key),
    dataset_name VARCHAR,
    dataset_description VARCHAR,
    metric_name VARCHAR,
    metric_value FLOAT,
    metric_details VARIANT,
    evaluated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Simplified Core Tables (Quick Start)

For basic use cases, use these simplified core tables:

| Table | Description | Key Fields |
|-------|-------------|------------|
| `dicom_patient` | Patient demographics | patient_id, patient_name, birth_date, sex |
| `dicom_study` | Imaging study (exam) | study_instance_uid, study_date, study_description, accession_number |
| `dicom_series` | Image series within study | series_instance_uid, study_instance_uid, modality, series_description |
| `dicom_instance` | Individual DICOM image | sop_instance_uid, series_instance_uid, instance_number, image_type |
| `dicom_equipment` | Imaging equipment info | station_name, manufacturer, model_name, software_versions |

## Common DICOM Tags Reference

### Patient Module (0010,xxxx)
| Tag | Name | VR | Description |
|-----|------|-----|-------------|
| (0010,0010) | PatientName | PN | Patient's name |
| (0010,0020) | PatientID | LO | Patient identifier |
| (0010,0021) | IssuerOfPatientID | LO | Issuer of patient ID |
| (0010,0030) | PatientBirthDate | DA | Birth date |
| (0010,0040) | PatientSex | CS | M, F, or O |
| (0010,1001) | OtherPatientNames | PN | Alternative names |
| (0010,1000) | OtherPatientIDs | LO | Alternative patient IDs |

### Study Module (0008,xxxx / 0020,xxxx)
| Tag | Name | VR | Description |
|-----|------|-----|-------------|
| (0020,000D) | StudyInstanceUID | UI | Unique study identifier |
| (0008,0020) | StudyDate | DA | Date study started |
| (0008,0030) | StudyTime | TM | Time study started |
| (0008,1030) | StudyDescription | LO | Study description |
| (0008,0050) | AccessionNumber | SH | RIS accession number |
| (0008,0090) | ReferringPhysicianName | PN | Referring physician |

### Series Module
| Tag | Name | VR | Description |
|-----|------|-----|-------------|
| (0020,000E) | SeriesInstanceUID | UI | Unique series identifier |
| (0020,0011) | SeriesNumber | IS | Series number |
| (0008,0060) | Modality | CS | Type of equipment (CT, MR, etc.) |
| (0008,103E) | SeriesDescription | LO | Series description |
| (0018,0015) | BodyPartExamined | CS | Body part examined |
| (0020,0052) | FrameOfReferenceUID | UI | Frame of reference |
| (0018,5100) | PatientPosition | CS | Patient positioning |

### Image Module
| Tag | Name | VR | Description |
|-----|------|-----|-------------|
| (0008,0018) | SOPInstanceUID | UI | Unique image identifier |
| (0008,0016) | SOPClassUID | UI | SOP Class identifier |
| (0020,0013) | InstanceNumber | IS | Image number in series |
| (0008,0008) | ImageType | CS | Image type components |
| (0028,0010) | Rows | US | Image height in pixels |
| (0028,0011) | Columns | US | Image width in pixels |
| (0028,0100) | BitsAllocated | US | Bits allocated per pixel |
| (0028,0101) | BitsStored | US | Bits stored per pixel |

### Equipment Module
| Tag | Name | VR | Description |
|-----|------|-----|-------------|
| (0008,0070) | Manufacturer | LO | Device manufacturer |
| (0008,1090) | ManufacturerModelName | LO | Model name |
| (0018,1000) | DeviceSerialNumber | LO | Serial number |
| (0018,1020) | SoftwareVersions | LO | Software versions |
| (0008,0080) | InstitutionName | LO | Institution name |
| (0008,1010) | StationName | SH | Station identifier |

## Modality Codes

| Code | Modality | Code | Modality |
|------|----------|------|----------|
| CT | Computed Tomography | US | Ultrasound |
| MR | Magnetic Resonance | NM | Nuclear Medicine |
| CR | Computed Radiography | PT | PET |
| DX | Digital Radiography | XA | X-Ray Angiography |
| MG | Mammography | SEG | Segmentation |
| SR | Structured Report | OT | Other |

## Workflow

### Step 1: Analyze DICOM Files

```python
python scripts/analyze_dicom.py ./dicom_folder --recursive
```

Output shows:
- File count by modality
- Study/series/instance counts
- Date ranges
- Equipment manufacturers
- Missing required tags

### Step 2: Parse Metadata

```python
python scripts/parse_dicom.py ./dicom_folder \
    --recursive \
    --output-dir ./dicom_output \
    --format csv
```

### Step 3: Create Snowflake Schema

```sql
-- Run DDL statements to create tables
-- See "Comprehensive DICOM Data Model" section above
```

### Step 4: Load to Snowflake

```sql
CREATE OR REPLACE STAGE dicom_stage;
PUT file://./dicom_output/*.csv @dicom_stage;

COPY INTO dicom_patient 
FROM @dicom_stage/patient.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

## Sample Analytics Queries

### Study Volume by Modality
```sql
SELECT 
    ser.modality,
    COUNT(DISTINCT st.study_key) AS study_count,
    COUNT(DISTINCT i.instance_key) AS image_count
FROM dicom_study st
JOIN dicom_series ser ON st.study_key = ser.study_key
JOIN dicom_instance i ON ser.series_key = i.series_key
GROUP BY ser.modality
ORDER BY study_count DESC;
```

### Daily Exam Trends
```sql
SELECT 
    study_date,
    COUNT(*) AS exams,
    COUNT(DISTINCT patient_key) AS unique_patients
FROM dicom_study
WHERE study_date >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY study_date
ORDER BY study_date;
```

### Equipment Utilization
```sql
SELECT 
    e.manufacturer,
    e.manufacturer_model_name,
    e.station_name,
    COUNT(DISTINCT ser.series_key) AS series_count,
    COUNT(DISTINCT st.study_key) AS study_count
FROM dicom_equipment e
JOIN dicom_series ser ON e.series_key = ser.series_key
JOIN dicom_study st ON ser.study_key = st.study_key
GROUP BY 1, 2, 3
ORDER BY series_count DESC;
```

### Dose Analysis (CT)
```sql
SELECT 
    st.study_date,
    ser.series_description,
    d.ctdi_vol,
    d.dose_length_product,
    d.kvp,
    d.xray_tube_current
FROM dicom_dose_summary d
JOIN dicom_series ser ON d.series_key = ser.series_key
JOIN dicom_study st ON ser.study_key = st.study_key
WHERE ser.modality = 'CT'
ORDER BY st.study_date DESC;
```

## Privacy Considerations

DICOM files contain PHI (Protected Health Information). Always:

1. **De-identify before loading**: Remove or hash patient identifiers
2. **Use secure staging**: Encrypt data at rest
3. **Audit access**: Enable Snowflake access history
4. **Mask sensitive columns**: Use dynamic data masking

### De-identification Script

```python
python scripts/deidentify_dicom.py ./dicom_folder \
    --output-dir ./deidentified \
    --retain-dates  # Keep dates but shift by random offset
```

## Requirements

```
pydicom>=2.4.0
pandas>=2.0.0
snowflake-connector-python>=3.0.0
python-gdcm>=3.0.0  # For compressed transfer syntaxes
```

## Reference Files

- `references/dicom_tags_reference.md` - Complete DICOM tag dictionary
- `references/modality_specific_tags.md` - Tags by imaging modality
- `references/transfer_syntaxes.md` - DICOM transfer syntax reference
