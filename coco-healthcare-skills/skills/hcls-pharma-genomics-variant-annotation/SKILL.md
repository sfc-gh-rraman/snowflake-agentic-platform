---
name: hcls-pharma-genomics-variant-annotation
description: Annotate genomic variants with clinical and population databases. Use when annotating VCF files with ClinVar pathogenicity, gnomAD allele frequencies, gene information, or functional predictions. Triggers include VCF annotation, ClinVar, gnomAD, variant interpretation, pathogenic variants, allele frequency, ACMG classification, variant filtering.
---

# Variant Annotation

Annotate genomic variants (VCF) with clinical significance, population frequencies, and functional predictions.

## When to Use This Skill

- Annotating VCF files from sequencing pipelines
- Adding ClinVar pathogenicity classifications
- Filtering variants by population frequency (gnomAD)
- Prioritizing variants for clinical interpretation
- Adding gene/transcript annotations

## Annotation Sources

| Source | Purpose | Key Fields |
|--------|---------|------------|
| **ClinVar** | Clinical significance | CLNSIG, CLNDN (disease), review status |
| **gnomAD** | Population frequencies | AF (allele freq), AF_popmax, homozygote count |
| **dbSNP** | Variant IDs | rsID |
| **VEP/SnpEff** | Functional impact | Gene, consequence, IMPACT, protein change |
| **CADD** | Deleteriousness score | PHRED score (>20 = deleterious) |
| **SpliceAI** | Splice predictions | Delta scores for splice sites |

## Quick Start

### Option 1: Using VEP (Recommended)

```bash
# Install Ensembl VEP
# See references/vep_installation.md for details

# Annotate with VEP + plugins
vep -i input.vcf -o annotated.vcf \
    --cache --offline \
    --assembly GRCh38 \
    --vcf \
    --plugin ClinVar,clinvar.vcf.gz \
    --plugin gnomADc,gnomad.genomes.v4.vcf.gz \
    --plugin CADD,whole_genome_SNVs.tsv.gz
```

### Option 2: Using Python Script

```bash
# Annotate VCF with ClinVar
python scripts/annotate_variants.py input.vcf \
    --clinvar clinvar.vcf.gz \
    --gnomad gnomad.vcf.gz \
    --output annotated.vcf
```

### Option 3: Snowflake-Based Annotation

```sql
-- Load VCF to Snowflake variant table
-- Join with annotation reference tables
-- See references/snowflake_annotation.md
```

## Downloading Annotation Databases

### ClinVar

```bash
# Download latest ClinVar VCF
wget https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz
wget https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi
```

### gnomAD v4

```bash
# gnomAD is large - download specific chromosomes or use Hail
# Full genome: ~500GB
gsutil cp gs://gcp-public-data--gnomad/release/4.0/vcf/genomes/gnomad.genomes.v4.0.sites.chr*.vcf.bgz .
```

### dbSNP

```bash
wget https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz
```

## Variant Filtering Strategies

### Clinical Filtering (Pathogenic Variants)

```python
# Filter for clinically significant variants
filters = {
    'clinvar_sig': ['Pathogenic', 'Likely_pathogenic'],
    'gnomad_af': '<0.01',  # Rare in population
    'impact': ['HIGH', 'MODERATE'],
    'review_status': ['criteria_provided', 'reviewed_by_expert_panel']
}
```

### Research Filtering (Novel Variants)

```python
# Filter for rare, potentially impactful variants
filters = {
    'gnomad_af': '<0.001',  # Very rare
    'cadd_phred': '>20',    # Predicted deleterious
    'not_in_clinvar': True,  # Novel
}
```

## Output Formats

### Annotated VCF

Standard VCF with INFO fields added:

```
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="ClinVar significance">
##INFO=<ID=gnomAD_AF,Number=A,Type=Float,Description="gnomAD allele frequency">
##INFO=<ID=CADD_PHRED,Number=A,Type=Float,Description="CADD PHRED score">
#CHROM POS ID REF ALT ... INFO
chr1  12345  rs123  A  G  ...  CLNSIG=Pathogenic;gnomAD_AF=0.0001;CADD_PHRED=25.3
```

### Tabular Output (TSV/CSV)

```
CHROM  POS     REF  ALT  GENE    CONSEQUENCE       CLINVAR_SIG  GNOMAD_AF  CADD
chr1   12345   A    G    BRCA1   missense_variant  Pathogenic   0.0001     25.3
chr17  41245466 G   A    BRCA1   stop_gained       Pathogenic   0.00002    38.0
```

### Snowflake Table

```sql
CREATE TABLE annotated_variants (
    chrom VARCHAR,
    pos INTEGER,
    ref VARCHAR,
    alt VARCHAR,
    rsid VARCHAR,
    gene_symbol VARCHAR,
    consequence VARCHAR,
    hgvsc VARCHAR,
    hgvsp VARCHAR,
    clinvar_sig VARCHAR,
    clinvar_disease VARCHAR,
    clinvar_review VARCHAR,
    gnomad_af FLOAT,
    gnomad_af_popmax FLOAT,
    gnomad_hom INTEGER,
    cadd_phred FLOAT,
    impact VARCHAR,
    sample_id VARCHAR,
    genotype VARCHAR,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## ClinVar Significance Levels

| CLNSIG | Meaning | Action |
|--------|---------|--------|
| Pathogenic | Disease-causing | Report to clinician |
| Likely_pathogenic | Probably disease-causing | Report with caveat |
| Uncertain_significance (VUS) | Unknown | May need functional studies |
| Likely_benign | Probably not disease-causing | Usually filter out |
| Benign | Not disease-causing | Filter out |
| Conflicting | Multiple interpretations | Manual review needed |

## gnomAD Population Codes

| Code | Population |
|------|------------|
| afr | African/African American |
| amr | Latino/Admixed American |
| asj | Ashkenazi Jewish |
| eas | East Asian |
| fin | Finnish |
| nfe | Non-Finnish European |
| sas | South Asian |
| oth | Other |

## ACMG Classification Support

For ACMG/AMP variant classification:

```python
# Evidence codes from annotations
evidence = {
    'PVS1': consequence in ['frameshift', 'stop_gained', 'splice_donor', 'splice_acceptor'],
    'PS1': clinvar_pathogenic_same_aa_change,
    'PM2': gnomad_af < 0.0001 or absent,
    'PP3': cadd_phred > 25,
    'BP4': cadd_phred < 15,
    'BA1': gnomad_af > 0.05,
}
```

## Best Practices

1. **Use matched genome build**: Ensure VCF and databases use same reference (GRCh37 vs GRCh38)
2. **Normalize variants**: Left-align and decompose multi-allelic sites before annotation
3. **Check ClinVar review status**: Prioritize variants with expert review
4. **Consider population**: Use population-specific allele frequencies when available
5. **Combine evidence**: No single annotation is definitive; combine multiple sources

## Reference Files

- `references/vep_installation.md` - VEP setup guide
- `references/annotation_databases.md` - Database download links
- `references/snowflake_annotation.md` - Snowflake-based annotation workflow
- `references/acmg_criteria.md` - ACMG classification criteria

## Requirements

```
cyvcf2>=0.30.0
pandas>=2.0.0
pysam>=0.21.0
```
