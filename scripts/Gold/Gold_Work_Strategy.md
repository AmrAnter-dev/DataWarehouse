# Gold Layer Development Best-Practice Workflow

## 1. Understand Business Requirements

Start with the business use case, not the data.

Define:

- Business objectives  
- Reporting and analytics requirements  
- KPIs and metrics  
- Stakeholders and consumers (BI, analytics, ML)  
- Required data marts

**Goal:** Design Gold models around business consumption.

---

## 2. Identify the Analytical Data Model

Define the presentation model appropriate for the use case:

- Star Schema  
- Snowflake Schema (if justified)  
- Dimensional Data Mart  
- Aggregated Reporting Tables  
- Semantic / Metrics Layer structures

Document:

- Facts  
- Dimensions  
- Grain  
- Measures  
- Business definitions

---

## 3. Define Business Logic and Metric Rules

Standardize metric calculations before implementation.

Examples:

- Revenue  
- Gross margin  
- Customer lifetime value  
- Average order value  
- Retention metrics

Document:

- Formula logic  
- Filters and exclusions  
- Calculation assumptions  
- Metric ownership

**Single source of truth starts here.**

---

## 4. Source and Integrate from Silver Layer

Consume validated and conformed Silver models.

Use Silver as source for:

- Cleansed dimensions  
- Integrated facts  
- Standardized reference data

Avoid pushing cleansing logic into Gold.

Gold should model and serve, not repair.

---

## 5. Build Dimensional Models

Develop presentation-ready structures:

- Fact tables  
- Dimension tables  
- Aggregate fact tables  
- Summary marts

Apply:

- Surrogate keys  
- Conformed dimensions  
- Slowly changing dimension handling  
- Consistent naming standards

---

## 6. Apply Business-Level Transformations

Perform transformations oriented to analytics:

- KPI derivations  
- Aggregations  
- Business flags  
- Segmentation logic  
- Derived measures

Examples:

- customer_lifetime_value  
- gross_profit_margin  
- churn_flag  
- sales_growth_pct

---

## 7. Optimize for Query Performance

Design Gold for consumption performance.

Techniques:

- Partitioning  
- Clustering / indexing  
- Materialized views  
- Aggregate tables  
- Query optimization

Goal:

Fast, scalable analytical workloads.

---

## 8. Implement Data Quality and Reconciliation

Validate Gold outputs against source expectations.

Checks may include:

- Revenue reconciliation  
- Fact-to-dimension integrity  
- Aggregate balancing  
- KPI validation tests  
- Completeness checks

---

## 9. Add Audit and Metadata

Include observability and governance metadata:

- dwh_created_ts  
- dwh_last_updated_ts  
- source_system  
- refresh_batch_id  
- data_quality_status

---

## 10. Documentation and Semantic Layer Readiness

Document:

- Business glossary  
- Metric definitions  
- Data mart lineage  
- Schema diagrams  
- BI semantic model mappings

Prepare Gold to feed tools like:

- Power BI  
- Tableau  
- Looker  
- Semantic / metrics layers

---

# Rule of Thumb

**Requirements → Model → Define Metrics → Integrate → Build Marts → Transform → Optimize → Validate → Audit → Document**
