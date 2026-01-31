# Big Data Analytics – E-commerce Project

### Project Overview

This project implements a distributed multi-model data analytics system for e-commerce data using MongoDB, Apache HBase, and Apache Spark. The system demonstrates how heterogeneous, large-scale e-commerce data can be generated, stored, integrated, and analyzed using appropriate big data technologies.

Synthetic datasets are generated to simulate realistic e-commerce behavior, including users, products, browsing sessions, and transactions. MongoDB is used for semi-structured operational data, Apache HBase for high-volume session and transactional data, and Apache Spark for scalable batch analytics and integrated cross-system analysis.

### System Architecture

The project follows a layered, multi-model architecture:

1. Data Generation Layer

Python-based scripts generate synthetic e-commerce datasets.

2. Data Storage Layer

MongoDB: Stores users, products, and metadata in document-oriented format.

Apache HBase: Stores large-scale session and transactional data using a wide-column model.

3. Data Processing Layer

Apache Spark: Performs batch processing, Spark SQL analytics, and integrated analysis across MongoDB and HBase.

4. Analytics & Visualization Layer

Generates analytical outputs, dashboards, and business insights.

### Project Structure

```text
project-root/
├── data/
│   └── raw/
│       ├── users.json
│       ├── products.json
│       ├── categories.json
│       ├── transactions.json
│       └── sessions_*.json
│
├── mongodb/
│   ├── mongodb_loader.py
│   └── run_analytics.py
│
├── hbase/
│   ├── sessions_loader.py
│   └── hbase_schema.md
│
├── spark/
│   ├── analytics.py
│   ├── funnel_analysis.py
│   ├── spark_sql_analytics.py
│   └── sessions_to_events.py
│
├── dataset_generator.py
├── README.md
│
└── report/
    └── Final Exam Project_Big Data Analytics Report.pdf



### Dataset Generation

The dataset is generated using the script:

--> python dataset_generator.py

This script produces synthetic e-commerce data, including:

1. Users

2. Products and categories

3. Browsing sessions (2 million records)

4. Transactions

Generated datasets are stored in JSON format under the data/raw/ directory.

⚠️ Large datasets, Spark outputs, and Parquet files are intentionally excluded
from this GitHub repository.

This includes:
- Raw JSON datasets
- Spark-generated Parquet files
- Analytics output folders

These files are generated dynamically by running the pipeline.


### MongoDB Setup and Data Loading

MongoDB stores semi-structured operational data such as users and products.

1. Start MongoDB

Ensure MongoDB is running locally: mongod

2. Load Data into MongoDB

Run the MongoDB loader script:
--> python mongodb/mongodb_loader.py

This script:

Loads users, products, and transactions into MongoDB

Converts ISO timestamp strings into BSON DateTime objects

Creates collections in the ecommerce_project database


### HBase Setup and Data Ingestion

Apache HBase is used for scalable session and transactional data storage.

1. Start HBase using Docker

--> docker run -d --name hbase-ecommerce \
  -p 2181:2181 \
  -p 16010:16010 \
  -p 9090:9090 \
  harisekhon/hbase

2. Access HBase Shell
--> docker exec -it hbase-ecommerce hbase shell

3. Create Sessions Table
--> create 'sessions', 'meta', 'geo', 'device', 'stats', 'events'

4. Load Session Data
--> python hbase/sessions_loader.py

This loads approximately 2 million user sessions into HBase.


### Apache Spark Analytics

Apache Spark is used for batch analytics and integrated data processing.

1. Run Batch Analytics
--> spark-submit spark/analytics.py

2. Conversion Funnel Analysis
--> spark-submit spark/funnel_analysis.py

3. Spark SQL Analytics
--> spark-submit spark/spark_sql_analytics.py

These scripts perform:

1. Conversion funnel analysis

2. User behavior segmentation

3. Product performance evaluation

4. Geographic and revenue-based analytics

5. Customer lifetime value calculations

Analytical results are saved as Parquet files for efficient querying.


### Visualizations and Insights

The project generates multiple visualizations, including:

1. Conversion funnel drop-off analysis

2. Sales and revenue trends

3. User segmentation

4. Product performance dashboards

5. Inventory optimization insights

These visual outputs support data-driven business recommendations discussed in the final report.



### Report

The full technical and analytical documentation is available in:

--> report/Final Exam Project_Big Data Analytics Report.pdf


### Conclusion

This project demonstrates the practical application of distributed big data technologies in an e-commerce analytics context. By combining MongoDB, Apache HBase, and Apache Spark, the system efficiently handles heterogeneous data and enables scalable analytics. The project reflects real-world big data engineering practices and fulfills all requirements of the Big Data Analytics final exam assignment.
