# Automate DWP MI Pack

## Description
This project automates the processing and visualisation of the DWP MI Pack, which provides critical business intelligence comparing AKG to other UK providers.  
The solution ingests CSV files received via email, loads the data into snowflake, transforms the data and visualises key metrics in Power BI for senior leadership teams (SLT).

## Current Business Problem
Previously, the DWP MI Pack had to be taken at face value or processed manually, which was:
- Time-consuming and prone to human error  
- Slower for SLT to access timely insights
- Limited for visual learners   
- Difficult to consolidate and transform for Power BI reporting  

## Solution
This project automates the entire workflow:  

1. **Receive MI Pack via Email**  
   - CSV file containing performance data.

<details>
<summary><strong>Click to view aggregation SQL code</strong></summary>

```sql
SELECT
    site_id,
    MONTH(report_date) AS month,
    SUM(starts) AS total_starts,
    SUM(completions) AS total_completions
FROM raw_performance_data
WHERE report_date >= DATEADD(month, -1, CURRENT_DATE)
GROUP BY site_id, MONTH(report_date);
```

2. **Load into Snowflake Stage**  
   - CSV is uploaded to a Snowflake stage for processing.

3. **ETL Process**  
   - Python script extracts, transforms, and loads the data into **five Snowflake tables**.  
   - SQL queries further transform the base tables into **three main presentation tables** for reporting.  

4. **Visualisation in Power BI**  
   - Presentation tables are loaded into Power BI to create dashboards for SLT.  
   - Provides visual insights on AKG performance relative to other UK providers.

## Impact
- Fully automated MI Pack processing, eliminating manual steps and providing visuals  
- Faster and more reliable reporting for SLT  
- Standardised and cleaned data, ready for BI visualisation  
- Easy to scale and adapt for future reporting needs  
