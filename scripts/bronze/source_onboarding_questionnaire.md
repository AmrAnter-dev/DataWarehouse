Excellent foundation. These are essentially **source discovery / source onboarding questions**, and in mature data warehouse projects they usually get organized into assessment domains.

I’d enrich and structure them like this:

# Source System Discovery Questions (Before Building a Data Warehouse)

## 1. Business & Data Ownership

Understand who owns the data and why it exists.

Key questions:

* Who owns the data (business owner, system owner, data steward)?
* Who produces the data and who consumes it?
* What business process does this data support?
  (Sales, Claims, Finance, Inventory, Healthcare encounters, etc.)

Additional important questions:

* What business rules govern this data?
* What KPIs or reporting depend on it?
* Are there data governance policies or compliance constraints?
* Are there known data quality issues?

---

## 2. Source Documentation & Metadata

Understand what documentation already exists.

Questions:

* Is there system documentation available?
* Is there source data documentation?
* Is there a data dictionary or data catalog?
* Is there an existing conceptual, logical, or physical data model?
* Are source-to-source dependencies documented?
* Are there ERDs available?
* Are naming conventions and code sets documented?

Artifacts to request:

* Data dictionary
* ER diagrams
* Source schema documentation
* API specs (if applicable)
* Data catalog entries

---

## 3. Architecture & Technology Stack

Understand the source technical landscape.

Questions:

* How is data stored?

  * Relational database?
  * Flat files?
  * APIs?
  * Event streams?

* What database technology is used?

  * SQL Server
  * Oracle
  * PostgreSQL
  * SAP
  * Mainframe, etc.

* What are the integration capabilities?

  * JDBC/ODBC
  * APIs
  * CDC connectors
  * Message queues
  * File drops (SFTP)

Additional questions:

* What is the source system architecture?
* Is the source on-prem or cloud?
* Are there downstream dependencies or shared databases?

---

## 4. Extract & Load Strategy

This is a major discovery area.

### Load Pattern

* Full load or incremental load?
* Is Change Data Capture (CDC) available?
* How are inserts, updates, deletes tracked?

---

### Data Scope & History

* How much history is required?
* All available history or last 5/10 years?
* Are there archival systems involved?
* Is historical backfill needed?

---

### Data Volumes

* Expected extract sizes?

  * GB?
  * TB?
  * Billions of rows?

* Daily/weekly/monthly growth rates?

* Peak volumes?

This drives architecture choices.

---

### Source Constraints

* Are there extract volume limitations?
* Are there query throttling limits?
* Does heavy extraction affect source performance?

Critical question:

* How do we avoid impacting source system performance?

Examples:

* Off-hours extraction
* Read replicas
* CDC instead of full scans
* Query pushdown optimization

---

## 5. Security & Access

Very important and often underestimated.

Questions:

* How is authentication handled?

  * User/password
  * OAuth tokens
  * SSH keys
  * Service principals

* Access restrictions?

  * VPN
  * IP whitelisting
  * Firewall rules

* Authorization model?

  * Row-level restrictions?
  * Sensitive fields?
  * Masked data?

Also ask:

* Any compliance requirements?

  * HIPAA
  * GDPR
  * PCI

---

## 6. Data Quality & Reliability (I'd add this — important missing category)

Questions:

* Are there missing data issues?
* Duplicate records?
* Late arriving data?
* Schema changes expected?
* How often does source data contain errors?

This heavily impacts Bronze design.

---

## 7. Operational & SLA Questions (also often missed)

Questions:

* Source refresh frequency?
* Extraction windows?
* SLA expectations?
* Acceptable latency?
* Expected downtime windows?

Example:

```text id="6lmf6p"
Daily batch?
Near real-time?
Every 15 minutes?
```

This affects ingestion architecture.

---

# A Senior Data Engineer usually groups discovery into this checklist:

```text id="agzjry"
1. Business & Ownership
2. Metadata & Documentation
3. Architecture & Technology
4. Extract & Load Strategy
5. Data Volumes & Performance Constraints
6. Security & Access
7. Data Quality Risks
8. Operational SLAs
```

---

## One question almost everyone forgets (but matters a lot)

Ask:

**What changes over time in this source system?**

Because schema evolution, code changes, and business rule changes often break pipelines.

---




