# Silver Layer Development Best-Practice Workflow

## 1. Understand & Draw the Integration Model
قبل أي transformation، افهم العلاقات بين مصادر البيانات والجداول:

**Draw:**Integration Model (Source-to-Source relationships)
- Identify source entities and relationships  
- *  
-identify:
  - Primary Keys  
  - Foreign Keys  
  - Business Keys  
  - Cardinality (1:1, 1:M, M:M)  
- Detect overlapping entities across systems (CRM, ERP, etc.)  
- Define conformed dimensions early

**Goal:** understanding relationship before cleaning and join
Examples of artifacts:
- Entity Relationship Diagram (ERD)  
- Source-to-Target Mapping (STM)  
- Bus Matrix  
- Integration Model Diagram  

---

## 2. Data Profiling
Analyze data quality issues before cleaning:

- Null analysis  
- Duplicate detection  
- Invalid formats  
- Outlier checks  
- Domain validation

---

## 3. Structural Standardization
Standardize schema and formats:

- Rename columns using naming conventions  
- Correct data types  
- Trim whitespace  
- Standardize casing  
- Remove unwanted characters

---

## 4. Data Cleansing
Resolve data quality problems:

- Handle NULLs  
- Correct dirty values  
- Standardize codes and categories  
- Fix invalid dates  
- Remove duplicates

---

## 5. Apply Business Rules Validation
Enforce domain logic:

- Validate prices, quantities, dates  
- Check referential integrity  
- Handle invalid or contradictory records

---

## 6. Derivations & Enrichment
Create value-added attributes:

- Derived columns  
- Flags and indicators  
- Surrogate keys  
- Calculated metrics  
- Technical / audit enrichment columns

Examples:

- `dwh_last_updated_ts` → last ETL update timestamp  
- `dwh_validation_error` → validation issue flag or error description  
- `dwh_hash_key` → hash-generated business key for change detection / deduplication

---

## 7. Deduplication & Survivorship
Apply record selection rules:

- Latest record wins  
- Trusted source precedence  
- Survivorship logic

---

## 8. Conformance Across Sources
Unify shared business entities:

- Customer conformance  
- Product conformance  
- Country / code harmonization

---

## 9. Audit & Metadata
Add control and observability:

- Load timestamps  
- Source identifiers  
- Batch IDs  
- Data quality flags

---

# Rule of Thumb

**Model → Profile → Standardize → Cleanse → Validate → Enrich → Deduplicate → Conform → Audit**
