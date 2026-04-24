General Principles

* Naming Conventions: Use snake_case, with lowercase letters and underscore(_) to separate words.
* Language: Use English for all names.
* Avoid Reserved Words: Do not use SQL reserved words as object names.

Table Naming Conventions

Bronze Rules
* All names must start with source system name, and tables names must match their original names without renaming.
* <sourcesystem>_<entity>
  * <sourcesystem> : Name of the source system (e.g., crm , erp )
  * <entity> : Exact table name from the source system.
  * Example : crm_customer_info ---> Customer information from the CRM system


_________


Silver Rules
* All names start with the source system name , and table names must match their original names without renaming
* <sourcesystem>_<entity>
  * <sourcesystem> : Nmae of the source system (e.g., crm , erp )
  * <entity> : Exact table name from the source system.
  * Example : crm_customer_info ---> Customer information from the CRM system

__________


Gold Rules
* All names must use meaningful, business-aligned names for tables, starting with the category prefix.
* <category>_<entity>
  * <category> : Describe the role of the table, such as dim_ (dimension table) or fact_ (fact table) or agg_ (aggregation table).
  * <entity> : Descriptive name of the table, aligned with the bussiness domain (e.g., customers , products , sales).
  * Examples:
      * dim_customers ---> dimension table for customers
      * fact_transaction_line ---> fact table for per transaction line

__________


Colume Naming Conventions

Surrogate Keys

* All primary keys in dimension tables must use the suffix _key.
* <table_name>_key
    * <table_name> : Refers to the name of the table or entity the key belongs to.
    * _key: A suffix indicating that this column is a surrogate key.
    * Example : customer_key ---> Surrogate key in the dim_customer tab
__________

Technical Columns
* All technical columns must start with the prefix dwh_, followed by a descriptive name indicating the column purpose.
* dwh_<column_name)
      * dwh:Prefix exclusively for system-generated metadata.
      * <column_name> : Descriptive name indicating the column's purpose.
      * Example : dwh_load_date ---> system-generated column used to store data when the record was loaded

___________

Stored Procedure

* All stored procedure used for loading data must follow the naming pattern : load_<layer>
* <layer> : Represents the layer being loaded, such as bronze , silver , gold.
*  Examples:
        * load_bronze ---> Stored procedure for loading data into bronze layer.

__________

Some Rules for good practising never to miss
__________
1. Keep It Simple and Consistent
   
The most important rule for any naming convention is to maintain simplicity and consistency across all data objects.
Use clear, descriptive names that any member of your data teams can understand.
Avoid obscure abbreviations or internal codes that might confuse people and create barriers to access.
 
For example,  use a more descriptive name like dim_manufacturing_facility.
This option is slightly longer but is instantly clear about the data it contains.
This approach promotes data consistency and makes the data warehouse more approachable for business users.
_________
2. Use Prefixes for Layers and Object Types
   
Adding prefixes to your object names makes it easy to identify the object's purpose and its place in the data architecture.
A layered approach is common in modern data warehousing.
You can use prefixes to distinguish between these layers and different object types.
Consider a prefix system like this for your warehouse design:

•	stg_: For staging tables that hold raw data from a data source.
•	int_: For intermediate tables used during the process to transform data.
•	dim_: For dimension tables that store descriptive attributes.
•	fact_: For a fact table that stores quantitative transactional data.
•	vw_: For views that provide a specific perspective on the data.
•	dm_: For tables in a data mart built for a specific business function.
Following this structure, a dimension table for customer data would be named dim_customer.
A view on top of that for a monthly sales report could be vw_monthly_sales_summary.
________
3. Include the Business Context in the Name

When naming data objects, it is helpful to include the business process or subject area they relate to.
This provides valuable context and helps analysts find relevant data more efficiently.
Aligning object names with business functions makes the data warehouse more intuitive.
For instance, you might have tables named based on their business domain.
This makes it easier to manage financial data separately from marketing data.
•	fact_sales_orders
•	dim_marketing_campaigns
•	dm_hr_employee_headcount
_________
4. Avoid Special Characters and Spaces

Stick to letters, numbers, and underscores for all object names. You should avoid spaces, hyphens, and other special characters.
These characters can cause syntax errors in SQL queries or may not be supported by all database platforms and business intelligence tools.
Keeping names short and simple, using only alphanumeric characters and underscores, is a core part of any robust warehouse naming standards.
This approach reduces technical friction. It allows your team to focus on analysis rather than troubleshooting query errors.
__________
5. Use Singular Nouns for Table Names
   
When creating table names, it is a common practice to use singular nouns instead of plural ones.
The logic is that the table itself represents a collection of a single entity type.
For example, a table named customer contains a collection of individual customer records.
This keeps the naming convention consistent and improves the logical flow of SQL queries.
An analyst can write FROM customer which reads more naturally than FROM customers.
While this can be a subject of debate, choosing one form and applying it consistently is the most important part of this design principle.
___________
6. Be Specific with Column Names
    
For column names, specificity is critical for clarity. Instead of a generic name like name, use customer_first_name or product_name.
This practice helps avoid ambiguity, especially when joining multiple tables in a query.
When a column is a foreign key, its name should match the primary key in the referenced table, such as customer_id.
This self-documenting style makes relationships between tables obvious without needing to consult documentation.
Naming a specific attribute clearly is fundamental to data integrity.
___________
7. Establish a Consistent Date and Time Format
    
When naming columns that contain date or time information, stick to a consistent format.
A common choice for date columns is to end them with _date, like order_date.
For timestamps that include both date and time, using a suffix like _at or _ts (e.g., created_at or updated_ts) is a good standard.
This consistent naming for data types makes it easy for anyone to identify date and time fields.
It also helps in applying date-specific functions correctly.
For partitions or files, using the ISO 8601 format (YYYYMMDD) is useful for chronological sorting.
___________
8. Avoid Using Reserved Words
    
Do not use words that are reserved keywords in SQL or your specific database system.
Words like SELECT, GROUP, ORDER, or USER can cause unexpected syntax errors in your queries.
Using them as object names or column names forces you to use delimiters (like double quotes), which makes code harder to write and read.
Every database has a list of reserved words. It is good practice for the data engineering team to be familiar with them. Avoiding these words is a simple way to prevent frustrating and unnecessary bugs





