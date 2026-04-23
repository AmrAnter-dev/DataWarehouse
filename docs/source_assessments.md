# Source System Assessment Template (Data Warehouse Discovery Checklist)

Use this template during source-system onboarding before designing Bronze ingestion pipelines.

---

# Source System Overview

## General Information

| Item               | Details                  |
| ------------------ | ------------------------ |
| Source System Name |                          |
| Business Domain    |                          |
| System Owner       |                          |
| Data Steward       |                          |
| Technical Contact  |                          |
| Environment(s)     | Dev / Test / Prod        |
| Source Type        | DB / API / File / Stream |

---

# 1. Business & Data Ownership

## Discovery Questions

* Who owns the data?
* Who creates, updates, and consumes the data?
* What business process does this source support?
* What critical reports or KPIs depend on it?
* Are there business rules governing this data?
* Are there compliance requirements (HIPAA, GDPR, etc.)?

## Notes

```text
```

---

# 2. Source Documentation & Metadata

## Available Documentation

Check availability:

* [ ] Data Dictionary
* [ ] ER Diagram
* [ ] Source Schema Documentation
* [ ] API Specification
* [ ] Data Catalog Entry
* [ ] Code/Reference Tables Documentation
* [ ] Source-to-Source Dependencies

## Data Model Notes

* Conceptual Model:
* Logical Model:
* Physical Model:

## Key Entities and Relationships

```text
```

---

# 3. Architecture & Technology Stack

## Storage Technology

* Database platform:
* File format(s):
* On-prem or cloud:
* Primary integration mechanism:

## Integration Capabilities

Check supported methods:

* [ ] JDBC/ODBC
* [ ] REST API
* [ ] CDC
* [ ] Message Queue / Streaming
* [ ] SFTP/File Drop

## Constraints / Dependencies

```text
```

---

# 4. Extract & Load Strategy

## Load Pattern

Choose one:

* [ ] Full Load
* [ ] Incremental Load
* [ ] CDC
* [ ] Hybrid

Questions:

* How are inserts tracked?
* How are updates tracked?
* How are deletes handled?
* Is watermarking available?

---

## Historical Scope

* Required history:
* Archive sources involved?
* Backfill required?

Notes:

```text
```

---

# 5. Data Volumes & Performance

## Volume Estimates

| Metric            | Value |
| ----------------- | ----- |
| Estimated Rows    |       |
| Initial Load Size |       |
| Daily Growth      |       |
| Peak Volume       |       |

## Performance Questions

* Are there extraction limits?
* Does heavy extraction affect source performance?
* Recommended extraction windows?
* Read replica available?

Mitigation approach:

```text
```

---

# 6. Security & Access

## Authentication

Check applicable methods:

* [ ] Username / Password
* [ ] OAuth Token
* [ ] SSH Keys
* [ ] VPN Access
* [ ] IP Whitelisting
* [ ] Service Principal

## Authorization Constraints

* Restricted tables?
* Sensitive columns?
* Masked data?
* Row-level security?

Notes:

```text
```

---

# 7. Data Quality Risk Assessment

## Known Risks

Check observed risks:

* [ ] Missing Data
* [ ] Duplicates
* [ ] Late Arriving Data
* [ ] Schema Drift
* [ ] Inconsistent Codes
* [ ] Poor Referential Integrity

Known issues:

```text
```

---

# 8. Operational SLAs

## Source Operational Characteristics

| Item                   | Value |
| ---------------------- | ----- |
| Refresh Frequency      |       |
| Extraction Window      |       |
| Expected Latency       |       |
| Source Downtime Window |       |
| SLA Requirement        |       |

---

# 9. Recommended Bronze Ingestion Design (To Be Determined)

## Proposed Approach

* Ingestion Pattern:
* Partition Strategy:
* Metadata Columns:
* Validation Controls:
* Error/Quarantine Handling:

Notes:

```text
```

---

# Sign-Off

| Role           | Name | Date |
| -------------- | ---- | ---- |
| Business Owner |      |      |
| Source SME     |      |      |
| Data Engineer  |      |      |
| Architect      |      |      |

---

## Deliverables Produced from This Assessment

* Source-to-Bronze Mapping
* Ingestion Design Specification
* Data Quality Rules
* Access Requirements
* Risk Register
* Load Strategy Decision

---

## Suggested Repo Placement

```text
/docs/source_assessments/
   crm_source_assessment.md
   erp_source_assessment.md
   claims_source_assessment.md
```

This template is intended to be completed before Bronze layer development begins.
