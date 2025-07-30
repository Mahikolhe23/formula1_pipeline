# 🏎️ Formula 1 Data Pipeline — End-to-End Project

A complete **Formula 1 Analytics Pipeline** built using **Azure Data Factory, ADLS Gen2, and Azure Databricks**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🔧 Tech Stack

- **ADF** – Orchestrates pipeline
- **ADLS Gen2** – Raw + Processed Data Storage
- **Azure Databricks** – Data Cleaning, Transformation & Aggregation (PySpark)
- **Delta Lake** – Handles Medallion layers

---

## 🧱 Medallion Architecture

- **🔶 Bronze Layer**: Raw ingestion (CSV/JSON from API or storage)
- **⚪ Silver Layer**: Cleaned & joined data with standard formats
- **🥇 Gold Layer**: Aggregated stats for final reporting

---

## 🚀 Features

- End-to-End orchestration using ADF
- Modular notebook-based transformations in Databricks
- Schema evolution with Delta Lake
- Optimized for analytical queries

---

## 📊 Use Case

Track F1 races, teams, drivers, circuits & results. Ideal for:
- Data engineering interviews
- Building strong Azure project portfolio

---

## 📁 Folder Structure
formula1-pipeline/
├── notebooks/ # Databricks notebooks (Bronze/Silver/Gold)
├── adf_pipeline/ # JSON templates for ADF pipelines
├── data/ # Sample input data (if available)
└── README.md


---

## 📸 Sample Outputs

- Top 10 Drivers by Wins  
- Circuit-wise Average Race Time  
- Constructor Rankings by Season

