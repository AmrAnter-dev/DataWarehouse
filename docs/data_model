1. Business Process Modeled

Sales_transaction_line

2. Declared Grain 

One row in fact_sales_transaction represents one product sold  per transaction line.



3. Star Schema

Document your fact and dimensions:

fact_sales_transaction
├── date_key
├── customer_key
├── product_key
├── store_key
├── extended_sales_amount
└── quantity

Dimensions:

dim_customer
dim_product
dim_date
dim_store

diagram in /diagram folder

4. Fact Table Types

we use:

Transaction Fact table

5. Dimension Design

Document:

Surrogate keys
Natural keys
Slowly Changing Dimensions (SCD Type 1 / 2)

Example:

dim_customer implements SCD Type 2
Tracks changes in customer segment and address
