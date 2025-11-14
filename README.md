# Automate DWP MI Pack

## Description
This project automates the processing and visualization of the DWP MI Pack, which provides critical business intelligence (BI) comparing AKG to other UK providers.  
The solution ingests CSV files received via email, transforms the data, loads it into Snowflake, and visualizes key metrics in Power BI for senior leadership teams (SLT).

## Current Business Problem
Previously, the DWP MI Pack had to be processed manually, which was:  
- Time-consuming and prone to human error  
- Slower for SLT to access timely insights  
- Difficult to consolidate and transform for Power BI reporting  

## Solution
This project automates the entire workflow:  

1. **Receive MI Pack via Email**  
   - CSV file containing performance and comparison data.

2. **Load into Snowflake Stage**  
   - CSV is uploaded to a Snowflake stage for processing.

3. **ETL Process**  
   - Python script extracts, transforms, and loads the data into **five Snowflake tables**.  
   - Further transforms consolidate the data into **three main presentation tables** for reporting.  

4. **Visualization in Power BI**  
   - Presentation tables are loaded into Power BI to create dashboards for SLT.  
   - Provides visual insights on AKG performance relative to other UK providers.

## Impact
- Fully automated MI Pack processing, eliminating manual steps  
- Faster and more reliable reporting for SLT  
- Standardized and cleaned data, ready for BI visualization  
- Easy to scale and adapt for future reporting needs  
