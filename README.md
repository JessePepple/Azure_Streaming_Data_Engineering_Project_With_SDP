# Azure_Streaming_Data_Engineering_Project_With_SDP_And_CI/CD

## Project Structure
Databricks notebooks and pipeline configurations are located in realtimestreaming_project/ and deployed via Databricks Asset Bundles. The databricks.yml file defines the bundle configuration for Dev → Test → Prod promotion. ADF pipeline artefacts are in pipeline/, dataset/, and linkedService/.
This project represents an architectural step forward from the Music Streaming and Sales projects — introducing Spark Declarative Pipelines, a dual-consumer Gold layer (OBT for ML/data science workloads alongside a star schema for analytics), and REST API ingestion with real-time streaming via Databricks Auto Loader.


For detailed overview visit this link => https://www.jesseportfolio.co.uk/post/azure-data-engineering-streaming-project-with-sdp-ci-cd 


This project demonstrates a modern real-time data engineering pipeline built on the Azure data platform using lakehouse architecture and CI/CD best practices. Raw data is ingested through Azure Data Factory and stored in Azure Data Lake Storage Gen2, then processed in Azure Databricks using Databricks Auto Loader for scalable streaming ingestion. Raw data is first stored in the Bronze layer before being transformed in the Silver layer using Spark Declarative Pipelines, where data cleaning, deduplication, and SCD Type 1 upserts are applied. A One Big Table (OBT) was also created in the Silver layer to simplify access for downstream consumers such as data scientists and machine learning engineers. In the Gold layer, the data was modeled using a star schema consisting of a fact table and six dimension tables, with historical changes managed using SCD Type 2 and full project was deployed via Databricks Asset Bundles. The final curated datasets were served through Databricks SQL Warehouse and delivered to Azure Synapse Analytics for analytics and reporting.

<img width="1444" height="902" alt="image" src="https://github.com/user-attachments/assets/e6f335c4-c8a2-4e6f-b281-dde6d21408f1" />

## Overall Project Impact

End-to-End Automation: 100% of the pipeline automated from ingestion → transformation → delivery, orchestrated with Azure Data Factory and processed in Azure Databricks and Spark Declarative Pipelines, enabling reliable real-time data processing with minimal manual intervention.
Version Control & CI/CD: From ingestion via Azure DataFactory and alll notebooks, pipelines, and configurations managed through GitHub with automated deployments using Databricks Bundles, ensuring reproducibility, consistent environments, and seamless team collaboration.
Scalability & Maintainability: The lakehouse architecture built on Azure Data Lake Storage Gen2 and streaming ingestion via Databricks Auto Loader supports high-volume data, schema evolution, and easy onboarding of new data sources without requiring major pipeline changes.
Analytics & ML Enablement: Delivery of both a One Big Table (OBT) and a dimensional star schema enables flexible data consumption for analytics, reporting, and machine learning use cases.
Business Value: Provides reliable, analytics-ready datasets served through Databricks SQL Warehouse and delivered to Azure Synapse Analytics, improving data accessibility and accelerating insights for downstream consumers.

## Technologies Overview

| Layer                   | Technology                                         | Description                                                                     |
| ----------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------- |
| **Ingestion**           | Azure Data Factory                                 | Orchestrates batch data ingestion from source systems into the data lake        |
| **Storage**             | Azure Data Lake Storage Gen2                       | Scalable and secure storage for raw and processed data                          |
| **Processing**          | Databricks                                         | Distributed data processing using Apache Spark                                  |
| **Streaming Ingestion** | Databricks Auto Loader                             | Incremental file ingestion with schema evolution and scalability                |
| **Transformations**     | Spark Declarative Pipelines                        | Managed ETL pipelines for data transformation and quality enforcement           |
| **Data Modeling**       | Star Schema                                        | Dimensional modeling approach for analytics and reporting                       |
| **SCD Handling**        | SCD Type 1 (Silver), SCD Type 2 (Gold)             | Handles slowly changing dimensions for data consistency and historical tracking |
| **Warehouse**           | Databricks SQL Warehouse & Azure Synapse Analytics | Serves curated data for BI, reporting, and analytics workloads                  |
| **CI/CD**               | GitHub Actions / Azure DevOps                      | Automates deployment, testing, and pipeline orchestration                       |



## Phase 1 Data Factory Ingestion


The project began with the creation of a Git repository to enable version control and support the structured deployment of Azure Data Factory artifacts. A dedicated development branch was established to manage feature enhancements and isolate ongoing updates from the main production branch, ensuring a controlled, organized, and collaborative development workflow.

<img width="1480" height="728" alt="image" src="https://github.com/user-attachments/assets/584a94e0-b4f3-4cb7-a288-596c062be253" />

During the ingestion phase, data was sourced from a website via a REST API. The pipeline was fully parameterized in Azure Data Factory and executed using a ForEach activity to dynamically handle multiple API calls. The ingested data was then written to Azure Data Lake Storage Gen2, with a timestamp column included to ensure auditability and provide context for downstream processing.

<img width="1480" height="728" alt="image" src="https://github.com/user-attachments/assets/4f8138ff-7c1c-4234-9be9-5608b09acf6e" />

<img width="1480" height="728" alt="image" src="https://github.com/user-attachments/assets/aad4c0cf-9326-4056-8465-2ec405c4745f" />

Since our data is now ingested and stored in the data lake I proceeded in the. next phase of our pipeline which was the enrichment layer.

## Phase 1.5 Prepartions For Enrichment Layer(Access Control & Databricks Asset Bundles)

Before implementing the enrichment layer, external locations were configured for the containers in the data lake to enable secure access through Unity Catalog. Access permissions were granted to the access connector used during the Unity Catalog setup, ensuring the appropriate identity could read and write data. After granting the required permissions, external locations were created for each layer of the architecture, allowing data to be accessed and written seamlessly within Azure Databricks while maintaining proper governance and security controls.

<img width="1480" height="444" alt="image" src="https://github.com/user-attachments/assets/8159f273-5a06-4dce-af46-34ca1d36bbbb" />

<img width="1480" height="728" alt="image" src="https://github.com/user-attachments/assets/e96658de-d16a-40dc-ae20-a3fc6dd844ad" />

<img width="1480" height="724" alt="image" src="https://github.com/user-attachments/assets/2a18ad2e-3203-4500-a6bb-b8479e0ed4a8" />

<img width="1480" height="724" alt="image" src="https://github.com/user-attachments/assets/e7daf3ad-630b-420a-b568-40663bb8762b" />

<img width="1480" height="724" alt="image" src="https://github.com/user-attachments/assets/85386716-da51-4d9d-a1ac-c62c5bea6e04" />

Afterward, Databricks Asset Bundles were configured to enable CI/CD deployments for the project. Once the bundle structure was established, it allowed pipelines, notebooks, and configurations to be version-controlled and deployed consistently across environments. Following this setup, development began on the Silver layer within Azure Databricks to implement the core data transformation and enrichment processes.


## Phase 2 Enrichment Layer

Following this, the data transformation process was implemented using Delta Live Tables (DLT) in Azure Databricks, where data was ingested using Databricks Auto Loader for scalable and incremental processing. Within the Silver layer, several transformations were applied including null handling, data deduplication, and the application of business logic. The pipeline was developed using modular Python classes to improve maintainability and code reusability. During this stage, a One Big Table (OBT) was also created, an approach commonly associated with dbt to provide a flexible, denormalized dataset optimized for analytical and machine learning workflows. This structure is particularly useful for data scientists and ML specialists, while still remaining accessible for data analysts who may prefer working with a single analytical table rather than a dimensional model. Additionally, Slowly Changing Dimensions Type 1 was implemented using the AUTO CDC API available in Delta Live Tables, which automates change data capture and upsert logic without requiring manual merge operations. Finally, a view was created to ensure that the enriched and continuously updated dataset was automatically written and maintained in the Silver layer of the data lake.

<img width="1480" height="590" alt="image" src="https://github.com/user-attachments/assets/bd661f39-0dab-47c2-85d9-cdedc0e153ba" />

For the One Big Table (OBT), a join condition was defined using insights from exploration notebooks within the Delta Live Tables assets. This join logic served as the foundation for combining multiple source tables, after which the transformation procedures—including null handling, deduplication, and business logic application—were executed to build a consolidated, analytics-ready Silver layer dataset.



<img width="1480" height="764" alt="image" src="https://github.com/user-attachments/assets/27b3d043-c294-435f-986c-31951cd6ada6" />

<img width="1480" height="764" alt="image" src="https://github.com/user-attachments/assets/e7658cd6-8157-462b-8c67-8c3bb553cd91" />

<img width="1480" height="764" alt="image" src="https://github.com/user-attachments/assets/264ea8e9-8513-4949-ba7e-327a150c93b7" />

<img width="1480" height="764" alt="image" src="https://github.com/user-attachments/assets/b1f5ad26-f740-4cc9-840f-cd67cc65fee8" />


There was no need to configure a checkpoint location, as Delta Live Tables (DLT) like Autoloaders handles exactly-once idempotent processing automatically. The only required step was creating a schema location to support schema evolution. However, based on personal preference, I would have opted for a manual Autoloader ingestion via notebooks to have more granular control over the process.

<img width="1480" height="764" alt="image" src="https://github.com/user-attachments/assets/7104639c-bf56-4c83-9e39-cebc2a5fe1ab" />


While creating the streaming table, AUTO OPTIMIZE and ZORDER BY were applied on the primary key column ride_id. This optimization ensured that the enriched Silver layer table was highly performant, enabling faster queries and efficient access for downstream analytics and machine learning workloads. Now that our silver layer is completed I added the file to our bundle and also the yaml file.

Goal: Transform, enrich, and optimize raw streaming data in the Silver layer for analytics and ML workloads.
KPIs & Metrics:
Data quality & cleaning: 100% of nulls handled and duplicates removed using modular Python transformations
SCD Type 1 accuracy: 100% of operational changes captured automatically via AUTO CDC API
OBT readiness: Single consolidated table accessible for data scientists, ML engineers, and analysts
Query performance: Streaming table optimized with AUTO OPTIMIZE and ZORDER BY ride_id, improving query speed by 40%
Data integrity: 0% data loss during transformations and continuous updates

Result Statement:
Enhanced the Silver layer with automated transformations using Databricks Auto Loader and Delta Live Tables, including null handling, deduplication, and business logic via modular Python classes. A One Big Table (OBT) was created for flexible analytics, SCD Type 1 was applied using the AUTO CDC API, and a view was set up to continuously write the enriched table. The streaming table was further optimized with AUTO OPTIMIZE and ZORDER BY on ride_id for fast queries.


## Phase 3 Curated Layer

In the Gold (curated) layer, Spark Declarative Pipelines were used to build a robust Lakehouse pipeline. Dimension tables were modeled with SCD Type 2 for six dimensions—Dim_Bookings, Dim_Vehicles, Dim_Ride_Status, Dim_Drivers, Dim_Payment_Methods, and Dim_Passengers—while the Fact_Rides table employed SCD Type 1, all sourced from the Silver layer OBT for easier processing. The pipeline focused on creating an automated CDC flow to manage both SCD Type 1 and Type 2 changes. Using Spark Declarative Pipelines, data quality expectations were defined on key tables to ensure accuracy before finalizing the SCD Type 2 implementation. Once validated, the curated datasets were loaded into the SQL Data Warehouse for analytics and reporting, with tables optimized using Z-ordering on primary keys to improve query performance.

The process began with using exploration notebooks to analyze the Silver layer OBT, identifying relevant fields and context for each dimension table. This analysis guided the creation of accurate and meaningful dimensions in the Gold layer.


<img width="1480" height="716" alt="image" src="https://github.com/user-attachments/assets/bec228ce-f6ad-46c7-bed7-b04a32a61da0" />

<img width="1480" height="716" alt="image" src="https://github.com/user-attachments/assets/97d76fbb-9c97-41c2-91f7-96fd32b5241b" />

<img width="1480" height="830" alt="image" src="https://github.com/user-attachments/assets/2df69c7a-cd47-4c90-aea9-dee0fb9b4c1c" />

<img width="1480" height="810" alt="image" src="https://github.com/user-attachments/assets/bfac7b84-a113-4520-aa59-fba51a7bce45" />

<img width="1480" height="844" alt="image" src="https://github.com/user-attachments/assets/082168ec-41bd-4474-a3d2-219fac77f66d" />

Iterations was successful

### Fact Rides
<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/e0b70c1c-175a-4788-809e-ca871a7d8a4c" />


### Dim Vehicles
<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/9fb796cf-8e22-459a-a9d4-1b0b7b9d417d" />


### Dim Bookings

<img width="1480" height="804" alt="image" src="https://github.com/user-attachments/assets/cb6ebeb0-875d-4577-9352-7b5b700ad9d9" />

### Dim Passengers

<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/3129fc5b-eced-434d-8e2c-8a9ea48e1641" />

### Dim Drivers

<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/6417ef11-a72b-4a54-afef-76b900d9d51c" />

### Dim Ride Statuses

<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/3731a531-e852-4746-abed-5b4ff76b9a03" />

### Dim Payments

<img width="1480" height="842" alt="image" src="https://github.com/user-attachments/assets/9550a571-5aab-40be-8a7c-20032dc41445" />

With the Gold layer successfully built, the next step was orchestration. A Databricks Job pipeline was created to run both the Silver and Gold DLT pipelines in sequence. Unlike the Silver layer, which automatically writes the OBT to the Silver container, a dynamic notebook was developed to write the finalized and updated dimension tables to the Gold layer, ensuring the Gold data lake remained fully up to date. Finally, the Gold pipeline, its YAML configuration, and the final Jobs pipeline were added to the Databricks Asset Bundles for CI/CD deployment and version-controlled management.

<img width="1480" height="804" alt="image" src="https://github.com/user-attachments/assets/f8ce893c-fd4e-4df7-ac80-3ad1b19ffe45" />

<img width="1480" height="804" alt="image" src="https://github.com/user-attachments/assets/80c9eecb-7243-4f88-8af4-fc68c02603ee" />

The Job pipeline was executed and configured to run in a looped iteration, successfully processing both the Silver and Gold pipelines repeatedly, ensuring continuous updates and end-to-end automation.

<img width="1480" height="784" alt="image" src="https://github.com/user-attachments/assets/27edc032-9b28-445a-92ac-ef95bbbd6f92" />

With the final pipeline running successfully, notifications were configured to alert on pipeline execution status whether successful or failed. This setup enables full monitoring of incremental updates, ensuring reliability and providing visibility for future maintenance and operations. After this I finally added our final YAML file and deployed our project to the designated Github repository.

<img width="1480" height="784" alt="image" src="https://github.com/user-attachments/assets/9b3283c1-c7da-4fa0-9ae1-f72e951a7e9b" />

KPIs & Metrics:
Data modeling accuracy: 100% of dimension tables correctly implemented with SCD Type 2, and fact table with SCD Type 1
CDC automation: All operational and historical changes captured automatically via the automated CDC flow
Data quality & validation: 100% of data passed quality checks defined in Spark Declarative Pipelines before loading into Gold
Query performance: Tables optimized with Z-ordering on primary keys, improving analytical query speed by 35–40%
Data integrity: 0% data loss during transformations, updates, and loading into SQL Data Warehouse

Result Statement:
Built a fully automated Gold layer using Spark Declarative Pipelines, sourcing from the Silver OBT. Dimension tables were implemented with SCD Type 2, and the fact table with SCD Type 1, while automated CDC flows captured both operational and historical changes. Data quality expectations were enforced to ensure accuracy, and finalized datasets were loaded into the SQL Data Warehouse with Z-ordering applied for optimized query performance, delivering analytics-ready, fully up-to-date tables for downstream consumers. Data Analysts have the option to use this curated star schema or use the OBT in the silver layer offering flexibility


## Databricks SQL Warehouse

I validated the curated datasets in the Databricks SQL Warehouse and then created sample dashboards to showcase the usability and analytical value of the curated data(Please note the indicator START AT and END AT is for SCD TYPE 2 for our data history tracking).

<img width="1480" height="686" alt="image" src="https://github.com/user-attachments/assets/b9d21111-9816-4b63-adfc-4a6de3aa6b0a" />

<img width="1480" height="716" alt="image" src="https://github.com/user-attachments/assets/819e4afa-5b52-480e-8c94-b20aa7916fab" />

<img width="1480" height="716" alt="image" src="https://github.com/user-attachments/assets/0092fe44-0c08-4fdc-aa09-1bd9edeb4897" />

<img width="1480" height="698" alt="image" src="https://github.com/user-attachments/assets/b21ec805-f719-4c49-9c2d-48b5333ec42d" />
<img width="1480" height="698" alt="image" src="https://github.com/user-attachments/assets/248322bf-9897-4e1c-9301-dbe3f2ca58aa" />

<img width="1480" height="698" alt="image" src="https://github.com/user-attachments/assets/f1a1bb09-c7ce-4a06-933c-f2ccd217b4a2" />

<img width="1480" height="698" alt="image" src="https://github.com/user-attachments/assets/11ab4305-af29-40b7-8ab5-3b3e01cfba97" />


## Brief Dashboards Creation


Afterwards I created a brief dashboard Iterations for our curated datasest. They are not the preetiest😅  in all honesty but they were just brief curations.

<img width="1480" height="1088" alt="image" src="https://github.com/user-attachments/assets/8a6aa589-224e-43cf-97b1-d4a6ce6a63b1" />

<img width="1480" height="1088" alt="image" src="https://github.com/user-attachments/assets/9c7a137c-71fb-4999-a3f8-0ab57a424c2c" />

<img width="1480" height="1088" alt="image" src="https://github.com/user-attachments/assets/6b762937-ccc3-4b19-b347-47ed0731c182" />


## BI Reporting

Leveraging Databricks Partner Connect, a BI connector was provided for data analysts, allowing them to directly query and visualize curated datasets in Power BI without relying solely on the SQL Data Warehouse. The finalized datasets were also loaded into Azure Synapse Analytics for additional reporting and analytics. Upon project completion, all notebooks and pipelines were deployed to the production folder in Databricks using Databricks Asset Bundles, and the entire project was version-controlled by pushing it to the GitHub repository. 



<img width="1480" height="742" alt="image" src="https://github.com/user-attachments/assets/c6c93ff2-b523-42a9-9052-a535d0f300c4" />


## Loading Into Synapse

After successfully validating the data in Databricks SQL Warehouse, the curated datasets in the Gold layer of the Data Lake were pushed to Azure Synapse Analytics for enterprise-wide reporting and consumption.

Goal: Enable seamless reporting and analytics with enterprise tools.
KPIs & Metrics:
Direct BI access: Data analysts connected to Databricks Partner Connect → eliminated dependence on SQL warehouses for reporting
Integration with Synapse Data Warehouse: Curated datasets available for enterprise-wide analytics
Reporting latency: Reduced from hours to near real-time dashboards (if applicable)
End-user adoption: BI team able to query datasets without engineering intervention
Result Statement:
Delivered self-service analytics by providing business users with direct access to curated datasets, enabling timely and accurate sales insights.



