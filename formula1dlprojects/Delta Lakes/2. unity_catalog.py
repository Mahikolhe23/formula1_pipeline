# Databricks notebook source
# MAGIC %md
# MAGIC #### Unity Catalog
# MAGIC Unity Catalog is a databricks offered unified solution for implementing data governance in the data lakehouse. Unity Catalog provides various features.
# MAGIC   1. Metastore
# MAGIC   2. Access Control
# MAGIC   3. Audit Logs
# MAGIC   4. Data Lineage
# MAGIC   5. Data Explorer
# MAGIC   6. User Management
# MAGIC
# MAGIC #### Data Governance
# MAGIC Data Governance is the process of managing the availability, usability, integrity and security of the data present in an enterprice, here are some general characterasticks of good data governance.
# MAGIC   1. controls access to the data for the users.
# MAGIC   2. ensures that the data is trust worthy and not misused.
# MAGIC   3. help implement privacy regulations such as GDPR, CCPA etc.
# MAGIC
# MAGIC key areas where unity catalog provide benefits for data governance
# MAGIC   1. data access control - only access user can get control to all service like adls, adf, adb, etc.
# MAGIC   2. data audit - can be audit data by access user.
# MAGIC   3. data linage - can get full data flow from file to dashboard and viceversa.
# MAGIC   4. data discoverability
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Unity Catalog Hierarchy
# MAGIC =======================
# MAGIC ```
# MAGIC Metastore
# MAGIC └── Catalog (e.g., `main`, `dev`, `sales_data`)
# MAGIC     ├── Schema / Database (e.g., `analytics`, `raw`, `processed`)
# MAGIC     │   ├── Table
# MAGIC     │   │   ├── Internal Table (Managed)
# MAGIC     │   │   └── External Table (Unmanaged - stored in external location like ADLS/S3)
# MAGIC     │   ├── View
# MAGIC     │   └── Function (UDFs / Scalar / Table functions)
# MAGIC     └── [Optional] Other schemas...
# MAGIC ```
# MAGIC
# MAGIC ## Definitions
# MAGIC - **Metastore**: Top-level container for Unity Catalog. Only one per region.
# MAGIC - **Catalog**: A logical collection of schemas, used to organize data (e.g., `main`, `dev`).
# MAGIC - **Schema (Database)**: A namespace within a catalog that holds tables, views, and functions.
# MAGIC - **Table**:
# MAGIC   - **Internal (Managed)**: Data stored and managed by Databricks.
# MAGIC   - **External (Unmanaged)**: Data stored in external locations (e.g., ADLS, S3), you manage lifecycle.
# MAGIC - **View**: Virtual table based on SQL queries.
# MAGIC - **Function**: User-defined functions (scalar/table) scoped to the schema.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Big Data Processing & Platform Evolution
# MAGIC ```
# MAGIC Apache Hadoop (2010)
# MAGIC └── Apache MapReduce
# MAGIC     └── Problems:
# MAGIC         - Slow batch processing
# MAGIC         - Hard to write complex logic in Java
# MAGIC         - Poor support for iterative ML jobs
# MAGIC
# MAGIC Apache Spark (2014+)
# MAGIC └── Replacement for MapReduce
# MAGIC     ├── Fast in-memory computation
# MAGIC     ├── Supports batch, streaming, ML
# MAGIC     └── Problems:
# MAGIC         - Complex cluster setup
# MAGIC         - No easy managed platform
# MAGIC         - No transactional data layer
# MAGIC
# MAGIC → PySpark
# MAGIC    └── Python API for Spark to simplify dev experience
# MAGIC    └── Still needs Spark cluster setup manually
# MAGIC
# MAGIC Databricks (2015+)
# MAGIC └── Unified analytics platform founded by creators of Spark
# MAGIC     ├── Provides managed Spark clusters
# MAGIC     ├── Simplified workspace (Notebook UI)
# MAGIC     ├── Collaboration-friendly
# MAGIC     └── Problems:
# MAGIC         - Still lacked ACID guarantees & governance
# MAGIC
# MAGIC Delta Lake (2019+)
# MAGIC └── Open-source storage layer over Parquet
# MAGIC     ├── Brings ACID transactions
# MAGIC     ├── Time travel, versioning
# MAGIC     ├── Schema enforcement & evolution
# MAGIC     └── Became default table format in Databricks
# MAGIC
# MAGIC → Delta Live Tables (DLT)
# MAGIC    └── ETL framework for declarative pipelines using Delta tables
# MAGIC
# MAGIC Unity Catalog (2022+)
# MAGIC └── Centralized metadata, access control & governance
# MAGIC     ├── Metastore → Catalog → Schema → Table/View/Function
# MAGIC     ├── Column-level security
# MAGIC     └── Solves multi-workspace governance, audit, access mgmt
# MAGIC ```

# COMMAND ----------

