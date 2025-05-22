# Automated Customer & Product Analytics ETL Pipeline

## Project Overview

This project implements an **automated Extract, Transform, Load (ETL) pipeline** using SQL to transform raw transactional data into structured, analyst-ready datasets. The primary goal is to provide timely, reliable, and actionable business insights into customer behavior, sales trends, and product performance by automating data aggregation and quality checks.

This pipeline effectively addresses the challenge of fragmented data, ensuring data consistency and significantly improving reporting efficiency for data-driven decision-making.

## Technologies Used

* **Database:** MySQL
* **Core Language:** SQL (Advanced)
* **Features Utilized:** Database Event Scheduling, Joins, Window Functions, Aggregations, Subqueries, Date/Time Functions, Data Modeling.

## Data Source

This project utilizes a sample database schema, conceptually similar to the **Chinook database**, which models a digital media store. It includes tables such as `customer`, `employee`, `invoice`, `invoiceline`, `track`, `album`, `artist`, `mediatype`, `genre`, `playlist`, and `playlisttrack`.

## Key Features & Deliverables

The ETL pipeline produces the following key analytical tables, which are designed for daily refresh:

1.  ### `customer_summary`
    * **Purpose:** Provides a consolidated 360-degree view of customer data.
    * **Details:** Includes customer demographics, contact information, total purchases, total sales, first and last purchase dates, and a calculated **customer status** (identifying "active" vs. "churned" customers based on recent activity).

2.  ### `product_summary_etl`
    * **Purpose:** Offers detailed insights into product (track) performance and engagement.
    * **Details:** Summarizes `TotalSales`, `TotalQuantity`, `TotalInvoices`, and `TotalCustomers` for each track. Enriches product information with artist, album, media type, genre, and a list of associated playlists.

3.  ### `sale_summary`
    * **Purpose:** Presents granular transaction-level data for in-depth sales analysis and trend tracking.
    * **Details:** Captures individual invoice line items with associated customer and track details, `UnitPrice`, `Quantity`, and `TotalSales`. This table also serves as the foundation for **Month-over-Month (MoM) sales trends** and **customer cohort analysis**.

4.  ### Automated Data Quality Checks & Logging (Implied in transformation scripts)
    * **Purpose:** Ensures data integrity and reliability by continuously monitoring for anomalies.
    * **Details:** Though not a separate output table shown in the `ls`, the ETL process includes logic for data quality checks (e.g., for multiple artists per album, missing identifiers, sales discrepancies between invoice and invoiceline). Results of these checks would ideally be logged to ensure data integrity and provide an audit trail for data health. *(If you have a `logs` table created/populated by your SQL, make sure to include its setup in one of your scripts)*

## ETL Process Overview

The project implements a daily automated ETL process:

* **Extract (E):** Data is pulled from the raw source tables (`customer`, `invoice`, `track`, etc.).
* **Transform (T):** Raw data is cleaned, validated, aggregated, and joined to create the summarized and denormalized `customer_summary`, `product_summary_etl`, and `sale_summary` tables. Data quality checks are performed during this phase.
* **Load (L):** The transformed data is loaded into the new analytical tables, overwriting previous daily snapshots to ensure freshness. This process is fully automated via MySQL's event scheduler.

## Project Structure

```
.
├── customer_summary.sql      # Script to create/update the customer_summary table and its automation event.
├── product_summary_etl.sql   # Script to create/update the product_summary_etl table and its automation event.
├── sale_summary.sql          # Script to create/update the sale_summary table (including MoM & cohort logic) and its automation event.
└── README.md
```

## Setup & Usage

To set up and run this ETL pipeline:

1.  **Prerequisites:**
    * MySQL Database (or a compatible relational database).
    * Ensure the `chinook_autoincrement` database schema is loaded with its standard data (or a similar schema mimicking the mentioned tables).

2.  **Enable MySQL Event Scheduler:**
    * The event scheduler must be `ON` for the daily automation to function.
    * Connect to your MySQL server and run:
        ```sql
        SHOW VARIABLES LIKE '%event%';
        SET GLOBAL event_scheduler = "ON";
        ```

3.  **Execute SQL Scripts:**
    * Execute each of the SQL scripts (`customer_summary.sql`, `product_summary_etl.sql`, `sale_summary.sql`) in your MySQL client. These scripts are designed to create the respective summary tables and set up their daily automation events.

    ```bash
    # Example command line execution for each script (adjust database name and path as needed)
    mysql -u your_user -p your_database_name < path/to/customer_summary.sql
    mysql -u your_user -p your_database_name < path/to/product_summary_etl.sql
    mysql -u your_user -p your_database_name < path/to/sale_summary.sql
    ```
    * **Note:** The events are typically set to run `EVERY 1 DAY`, so the first execution will happen at the next scheduled interval, and then daily thereafter. You can manually trigger an event for immediate testing if needed.

4.  **Verify Output:**
    * After the events have run (either automatically or manually triggered), query the newly created summary tables:
        ```sql
        SELECT * FROM customer_summary LIMIT 10;
        SELECT * FROM product_summary_etl LIMIT 10;
        SELECT * FROM sale_summary LIMIT 10;
        -- If you have a separate logs table, query it here:
        -- SELECT * FROM logs ORDER BY created_at DESC LIMIT 10;
        ```

## Future Enhancements

* Implement incremental loading instead of full table recreation for very large datasets to improve performance.
* Add more sophisticated anomaly detection and alert mechanisms for data quality issues.
* Integrate with a dashboarding tool (e.g., Tableau, Power BI) for direct visualization of these summary tables.
* Consider using more robust ETL orchestration tools for production environments (e.g., Airflow, Prefect).
* Containerize the database and ETL process using Docker for easier deployment.
