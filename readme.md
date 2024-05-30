
# 📊 End to End AWS Glue Data Pipeline

Welcome to the AWS Glue Data Pipeline project! This document provides an overview of the architecture, components, and workflow of the data pipeline.

## Architecture Overview 🏗️

Our data pipeline architecture leverages a combination of AWS services to ingest, process, and analyze data efficiently. Here's a breakdown of the key components and their roles:


1. **Source (REST API) 🌐**: Data is ingested from a REST API.

2. **Ingest Lambda 🟧**: AWS Lambda function that processes incoming data from the REST API and writes it to the landing layer.

3. **Landing Layer 🟩**: An S3 bucket where raw data is initially stored.

4. **Glue Job 🟪**: AWS Glue job that processes raw data and writes it to the bronze layer.

5. **Bronze Layer 🟩**: An S3 bucket that holds the processed data in its raw form.

6. **Glue Jobs and Delta Lake 🟪🔵**: Further processing of data is done through Glue jobs that transform the data into silver and gold layers, using Delta Lake for efficient data management.

7. **Silver Layer 🟩**: An S3 bucket for intermediate, cleansed, and structured data.

8. **Gold Layer 🟩**: An S3 bucket for refined, highly processed data ready for analysis.

9. **Failed DQ Layer 🟥**: An S3 bucket for data that fails data quality checks.

10. **Crawler and Catalogue 🗂️**: AWS Glue Crawler scans data in S3 buckets to populate the Glue Data Catalogue, making data available for querying.

11. **Athena, Redshift Serverless, and QuickSight 🔍📊**: Tools for querying and visualizing the data. Athena and Redshift Serverless provide query capabilities, and QuickSight is used for data visualization.

## Orchestration 🎛️

- **MWAA 🟣**: Managed Workflows for Apache Airflow orchestrates the workflow of data processing.

## Error Logging 🚨

- **EventBridge, Lambda, and DynamoDB 🟪🟧🟪**: Errors are captured by EventBridge and processed by a Lambda function, which logs the details into DynamoDB.

## CI/CD Pipeline 🚀

- **CodePipeline, CodeCommit, CodeBuild 🟢**: Continuous integration and continuous deployment (CI/CD) are managed by these services to automate code deployment and pipeline execution.

## Secrets Store 🔒

- **Secrets Manager 🔴**: Manages sensitive information like database credentials and API keys.

## Notifications 📬

- **SNS 🟣**: Simple Notification Service is used for sending notifications about pipeline status and alerts.

## Infrastructure as Code (IaC) 🛠️

- **CloudFormation 🟣**: Manages infrastructure deployment and updates through code.

## How It Works 🔄

1. **Data Ingestion**: Data is ingested from the source via a REST API and processed by the ingest Lambda function.
2. **Data Processing**: The data flows through various processing stages (landing, bronze, silver, gold) managed by AWS Glue jobs.
3. **Data Quality**: Data quality checks are performed, and any failed data is sent to the failed DQ layer.
4. **Data Cataloguing**: The Glue Crawler updates the Glue Data Catalogue with new and updated data.
5. **Data Analysis**: The processed and catalogued data can be queried using Athena or Redshift Serverless and visualized with QuickSight.
6. **Orchestration and Monitoring**: MWAA orchestrates the workflow, and EventBridge handles error logging and notifications.

## Conclusion 📈

This architecture ensures a robust, scalable, and efficient data pipeline capable of handling complex data processing tasks. With seamless integration of AWS services, it provides a reliable solution for your data engineering needs.

---

 
 > I will explain everything on [omkale.dev](https://omkale.dev/)