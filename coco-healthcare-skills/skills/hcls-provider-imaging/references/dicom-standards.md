# DICOM Standards Quick Reference

## What is DICOM?

Digital Imaging and Communications in Medicine (DICOM) is the international standard for medical imaging data. It defines:
- File formats for imaging data (`.dcm`)
- Communication protocols between imaging devices and systems
- Metadata structure (tags) embedded in each image

## DICOM Information Model (Hierarchy)

```
Patient
  └── Study (one exam visit)
       └── Series (one acquisition/sequence)
            └── Instance/Image (one image/frame)
```

## Key DICOM Tags for Snowflake Analytics

| Tag | Name | Typical Use |
|-----|------|-------------|
| (0010,0020) | PatientID | Patient matching, MDM |
| (0010,0010) | PatientName | PHI - requires masking |
| (0010,0030) | PatientBirthDate | PHI - de-identify |
| (0020,000D) | StudyInstanceUID | Unique study identifier |
| (0020,000E) | SeriesInstanceUID | Unique series identifier |
| (0008,0018) | SOPInstanceUID | Unique image identifier |
| (0008,0060) | Modality | CT, MR, XR, US, MG, PT, etc. |
| (0008,0020) | StudyDate | YYYYMMDD format |
| (0008,1030) | StudyDescription | Free text description |
| (0018,0015) | BodyPartExamined | Anatomical region |
| (0008,0080) | InstitutionName | Source facility |
| (0008,0090) | ReferringPhysicianName | PHI |
| (0028,0010) | Rows | Image height in pixels |
| (0028,0011) | Columns | Image width in pixels |
| (0028,0100) | BitsAllocated | Bit depth (8, 12, 16) |

## HIPAA Safe Harbor De-Identification

18 identifiers that MUST be removed/masked:
1. Names
2. Geographic data (smaller than state)
3. Dates (except year) related to an individual
4. Phone numbers
5. Fax numbers
6. Email addresses
7. Social Security numbers
8. Medical record numbers
9. Health plan beneficiary numbers
10. Account numbers
11. Certificate/license numbers
12. Vehicle identifiers and serial numbers
13. Device identifiers and serial numbers
14. Web URLs
15. IP addresses
16. Biometric identifiers
17. Full-face photographs
18. Any other unique identifying number/code

## Common Modality Codes

| Code | Modality |
|------|----------|
| CT | Computed Tomography |
| MR | Magnetic Resonance |
| XR | Digital Radiography |
| CR | Computed Radiography |
| US | Ultrasound |
| MG | Mammography |
| PT | PET (Positron Emission Tomography) |
| NM | Nuclear Medicine |
| DX | Digital X-Ray |
| RF | Radiofluoroscopy |
| OT | Other |

## DICOM-to-Snowflake Mapping Pattern

DICOM metadata is typically exported as JSON from PACS systems or DICOM toolkits (pydicom, dcmtk). Load as VARIANT in Snowflake and flatten:

```
DICOM File (.dcm) --> pydicom/dcmtk --> JSON export --> Snowflake Stage --> VARIANT column --> Flatten to relational
```

## FHIR ImagingStudy Resource

HL7 FHIR represents imaging studies as `ImagingStudy` resources:
- Maps to DICOM Study level
- Contains patient reference, modality list, series/instance details
- Can be loaded into Snowflake as JSON VARIANT and joined with DICOM metadata

## Data Volume Guidelines

| Scale | Studies/Year | Recommended Architecture |
|-------|-------------|------------------------|
| Small | < 100K | Single table, scheduled COPY |
| Medium | 100K - 1M | Dynamic Tables, incremental refresh |
| Large | > 1M | Streams + Tasks, partitioned tables, dedicated warehouse |
