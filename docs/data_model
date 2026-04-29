# Dimensional Modeling Design

## 1. Business Process Modeled

The business process modeled in this warehouse is:

**Sales Transaction Processing**

Specifically focused on capturing **sales transaction line-level events**, where each row represents an individual product sold within a transaction.

Business questions supported include:

- What products are selling the most?
- Who are the top customers by revenue?
- How do sales trend over time?
- Which stores drive the highest performance?

---

## 2. Declared Grain

**Grain Statement**

One row in `fact_sales_transaction` represents:

> **One product sold per transaction line**

This is the lowest level of detail stored in the fact table and establishes the foundation for all dimensional modeling decisions.

### Grain Implications
Because the grain is transaction-line level:

- Measures are fully additive  
- Sales can be analyzed across any dimension  
- Dimensions join consistently at the same level of detail  
- Aggregations (daily, monthly, customer-level, product-level) roll up correctly

---

## 3. Star Schema Design

### Fact Table

### `fact_sales_transaction`

| Column | Description |
|--------|-------------|
| date_key | Foreign key to date dimension |
| customer_key | Foreign key to customer dimension |
| product_key | Foreign key to product dimension |
| store_key | Foreign key to store dimension |
| extended_sales_amount | Sales amount at transaction-line level |
| quantity | Units sold |

---

### Dimension Tables

- `dim_customer`
- `dim_product`
- `dim_date`
- `dim_store`

---

## Star Schema Structure

```text
                    dim_date
                       |
                       |
dim_customer --- fact_sales_transaction --- dim_product
                       |
                       |
                   dim_store
```

Detailed schema diagram available in:

```text
/diagram
```

---

## 4. Fact Table Type

This model uses a:

## Transaction Fact Table

Characteristics:

- Captures atomic business events  
- Stores measurable transactional activity  
- Highly granular and fully additive  
- Supports drill-down and aggregation analysis

Typical measures:

- Sales amount  
- Quantity sold  
- Discount amount (if applicable)  
- Cost / margin (optional future extension)

---

## 5. Dimension Design

Dimensions are designed following dimensional modeling best practices.

### Surrogate Keys
Each dimension uses warehouse-generated surrogate keys:

- `customer_key`
- `product_key`
- `date_key`
- `store_key`

Used to:

- Decouple warehouse keys from source systems  
- Support historical tracking  
- Improve join performance

---

### Natural Keys
Business identifiers from source systems are retained as natural keys, for example:

- `customer_id`
- `product_id`
- `store_id`

Used for:

- Source traceability  
- Business reconciliation  
- Change detection

---

## Slowly Changing Dimensions (SCD)

### `dim_customer`
Implements:

**SCD Type 2**

Tracks historical changes for attributes such as:

- Customer segment  
- Customer address  
- Other slowly changing descriptive attributes

Typical Type 2 metadata:

- effective_start_date  
- effective_end_date  
- is_current_flag

---

## Modeling Summary

| Design Component | Selection |
|-----------------|-----------|
| Business Process | Sales Transactions |
| Grain | One product per transaction line |
| Fact Type | Transaction Fact |
| Schema Pattern | Star Schema |
| Customer Dimension | SCD Type 2 |

---
