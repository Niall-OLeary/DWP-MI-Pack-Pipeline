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

2. **Load into Snowflake Stage**  
   - CSV is uploaded to a Snowflake stage for processing.

3. **ETL Process**  
   - Python script extracts, transforms, and loads the data into **five Snowflake tables**.  
   - SQL queries further transform the base tables into **three main presentation tables** for reporting.
  <details>
<summary><strong>Click to view Python Scipt</strong></summary>

```Python
import pandas as pd
import numpy as np
from snowflake.snowpark.context import get_active_session

# Get the active Snowflake session
session = get_active_session()

# Read the CSV file from Snowflake stage
stage_file_path = '@dwp_stage/DWP_MI_PACK.csv'

# Download the file from stage to a temporary location
downloaded_files = session.file.get(stage_file_path, '/tmp/')

# The file will be downloaded to /tmp/ directory
local_file_path = '/tmp/DWP_MI_PACK.csv'

# Read the CSV file - only columns B through CC (columns 1-80, since A=0, B=1, etc.)
df = pd.read_csv(local_file_path, usecols=range(1, 81))

# Replace #REF! values with NaN/null
df = df.replace('#REF!', np.nan)

# Function to find table boundaries
def find_table_boundaries(df):
    """
    Identify where each table starts and ends based on empty rows
    """
    boundaries = []
    table_start = None
    
    for idx, row in df.iterrows():
        # Check if row is completely empty (all NaN or empty strings)
        is_empty = row.isna().all() or all(str(val).strip() == '' for val in row if pd.notna(val))
        
        if not is_empty and table_start is None:
            # Found start of a new table
            table_start = idx
            boundaries.append(idx)
        elif is_empty and table_start is not None:
            # Found end of current table (empty row after data)
            table_start = None
    
    return boundaries

# Find table boundaries
boundaries = find_table_boundaries(df)

# Function to extract individual tables
def extract_tables(df, boundaries):
    """
    Extract individual tables based on identified boundaries (empty row separation)
    """
    tables = {}
    
    for i, start_row in enumerate(boundaries):
        # Determine end row (next boundary or end of dataframe)
        if i + 1 < len(boundaries):
            # Find the next empty row before the next table
            end_row = boundaries[i + 1] - 1
            # Look backwards from next boundary to find last data row
            for j in range(boundaries[i + 1] - 1, start_row, -1):
                if not (df.iloc[j].isna().all() or all(str(val).strip() == '' for val in df.iloc[j] if pd.notna(val))):
                    end_row = j
                    break
        else:
            # For the last table, find the last row with data
            end_row = start_row
            for j in range(start_row, len(df)):
                if not (df.iloc[j].isna().all() or all(str(val).strip() == '' for val in df.iloc[j] if pd.notna(val))):
                    end_row = j
        
        # Extract table name from the first non-empty cell in the table
        table_name = f"Table_{i+1}"
        for col_idx in range(len(df.columns)):
            if pd.notna(df.iloc[start_row, col_idx]) and str(df.iloc[start_row, col_idx]).strip() != '':
                table_name = str(df.iloc[start_row, col_idx]).strip()
                break
        
        # Extract the table data
        table_data = df.iloc[start_row:end_row + 1].copy()
        
        # Clean up the table - remove completely empty rows
        table_data = table_data.dropna(how='all')
        
        # Promote first row to headers if table has more than 1 row
        if len(table_data) > 1:
            # Use the first row as column headers
            new_headers = []
            for idx, col in enumerate(table_data.columns):
                header_val = table_data.iloc[0][col]
                if pd.notna(header_val) and str(header_val).strip() != '':
                    # Rename first column to CPA, convert dates for others
                    if idx == 0:
                        new_headers.append('CPA')
                    else:
                        header_str = str(header_val).strip()
                        # Check if it's a numeric value that could be an Excel date serial
                        try:
                            # Try to convert to float to check if it's numeric
                            numeric_val = float(header_str)
                            # Excel date serial numbers are typically 5-digit numbers for recent dates
                            if 40000 <= numeric_val <= 50000:  # Rough range for 2009-2037
                                # Convert Excel serial date to datetime
                                # Excel epoch starts at 1900-01-01, but pandas uses 1899-12-30
                                date_val = pd.to_datetime('1899-12-30') + pd.Timedelta(days=numeric_val)
                                # Format as MMM-YY (e.g., Jan-21)
                                formatted_date = date_val.strftime('%b-%y')
                                new_headers.append(formatted_date)
                            else:
                                # Not a date serial number, keep as is
                                new_headers.append(header_str)
                        except (ValueError, TypeError):
                            # Not a number, keep original header
                            new_headers.append(header_str)
                else:
                    # Default name for empty headers
                    if idx == 0:
                        new_headers.append('CPA')
                    else:
                        new_headers.append(f'Column_{col}')
            
            table_data.columns = new_headers
            table_data = table_data.iloc[1:]  # Remove the header row from data
        
        # Reset index
        table_data = table_data.reset_index(drop=True)
        
        if not table_data.empty:
            tables[table_name] = table_data
    
    return tables

# Extract all tables
tables = extract_tables(df, boundaries)

# Save each table to Snowflake as tables
for table_name, table_df in tables.items():
    # Clean table name for Snowflake table naming conventions
    clean_name = table_name.replace(' ', '_').replace('/', '_').replace('-', '_').upper()
    
    # Convert pandas DataFrame to Snowpark DataFrame and save as table
    snowpark_df = session.create_dataframe(table_df)
    
    # Write to Snowflake table (replace existing if it exists)
    snowpark_df.write.mode("overwrite").save_as_table(f"DWP_{clean_name}")

print(f"\nSuccessfully created {len(tables)} Snowflake tables:")
for table_name in tables.keys():
    clean_name = table_name.replace(' ', '_').replace('/', '_').replace('-', '_').upper()
    print(f"  - DWP_{clean_name}")
```

</details>
<details>
<summary><strong>Click to view SQL Scipt</strong></summary>

```sql
-- This table computes the First Earnings Performance, Rolling Performance and Rank of each UK Provider against the DWP expectations.
create or replace table DATA_WAREHOUSE.BUSINESS_INTELLIGENCE.DWP_FE_BASE_DATA
as(
WITH base AS (
    SELECT
        e.cpa,
        TO_DATE(e."Date", 'MON-YY') as "Date",
        e."Date" as "Month-Year",
        e."Value" AS FE_Expected,
        a."Value" AS FE_Actual,
        DIV0(a."Value", e."Value") AS Performance
    FROM  DWP_OVERALL_FES_ACHIEVED a
    LEFT JOIN DWP_OVERALL_FEPI_EXPECTED e
        ON a.cpa = e.cpa 
       AND a."Date" = e."Date"
),

rolled AS (
    SELECT
        *,
        -- Rolling 3 months
        SUM(FE_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS FE_Expected_Rolling_3,
        SUM(FE_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS FE_Actual_Rolling_3,

        -- Rolling 6 months
        SUM(FE_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS FE_Expected_Rolling_6,
        SUM(FE_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS FE_Actual_Rolling_6,

        -- Rolling 12 months
        SUM(FE_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS FE_Expected_Rolling_12,
        SUM(FE_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS FE_Actual_Rolling_12,

        -- Contract-to-date
        SUM(FE_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FE_Expected_CTD,
        SUM(FE_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS FE_Actual_CTD
    FROM base
),

performance AS (
    SELECT
        *,
        DIV0(FE_Actual_Rolling_3,  FE_Expected_Rolling_3)   AS Performance_Rolling_3,
        DIV0(FE_Actual_Rolling_6,  FE_Expected_Rolling_6)   AS Performance_Rolling_6,
        DIV0(FE_Actual_Rolling_12, FE_Expected_Rolling_12)  AS Performance_Rolling_12,
        DIV0(FE_Actual_CTD,        FE_Expected_CTD)         AS Performance_CTD
    FROM rolled
)

SELECT
    *,
    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance DESC
         )
    END AS Performance_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_3 DESC
         )
    END AS Performance_Rolling_3_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_6 DESC
         )
    END AS Performance_Rolling_6_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_12 DESC
         )
    END AS Performance_Rolling_12_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_CTD DESC
         )
    END AS Performance_CTD_Rank
FROM performance
);


-- This table computes the TRNO (Tender Required Number of Outcomes) Performance, Rolling Performance and Rank of each UK Provider against the DWP expectations.
create or replace table DATA_WAREHOUSE.BUSINESS_INTELLIGENCE.DWP_TRNO_BASE_DATA
as(
WITH base AS (
    SELECT
        e.cpa,
        TO_DATE(e."Date", 'MON-YY') as "Date",
        e."Date" as "Month-Year",
        e."Value" AS JO_Expected,
        a."Value" AS JO_Actual,
        DIV0(a."Value", e."Value") AS Performance
    FROM  dwp_overall_outcomes_achieved a
    LEFT JOIN dwp_overall_tpl_expected e
        ON a.cpa = e.cpa 
       AND a."Date" = e."Date"
),

rolled AS (
    SELECT
        *,
        -- Rolling 3 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_3,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_3,

        -- Rolling 6 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_6,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_6,

        -- Rolling 12 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_12,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_12,

        -- Contract-to-date
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JO_Expected_CTD,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JO_Actual_CTD
    FROM base
),

performance AS (
    SELECT
        *,
        DIV0(JO_Actual_Rolling_3,  JO_Expected_Rolling_3)   AS Performance_Rolling_3,
        DIV0(JO_Actual_Rolling_6,  JO_Expected_Rolling_6)   AS Performance_Rolling_6,
        DIV0(JO_Actual_Rolling_12, JO_Expected_Rolling_12)  AS Performance_Rolling_12,
        DIV0(JO_Actual_CTD,        JO_Expected_CTD)         AS Performance_CTD
    FROM rolled
)

SELECT
    *,
    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance DESC
         )
    END AS Performance_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_3 DESC
         )
    END AS Performance_Rolling_3_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_6 DESC
         )
    END AS Performance_Rolling_6_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_12 DESC
         )
    END AS Performance_Rolling_12_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_CTD DESC
         )
    END AS Performance_CTD_Rank
FROM performance
);


-- This table computes the MRNO (Minimum Required Number of Outcomes) Performance, Rolling Performance and Rank of each UK Provider against the DWP expectations.
create or replace table DATA_WAREHOUSE.BUSINESS_INTELLIGENCE.DWP_MRNO_BASE_DATA
as(
WITH base AS (
    SELECT
        e.cpa,
        TO_DATE(e."Date", 'MON-YY') as "Date",
        e."Date" as "Month-Year",
        e."Value" AS JO_Expected,
        a."Value" AS JO_Actual,
        DIV0(a."Value", e."Value") AS Performance
    FROM  dwp_overall_outcomes_achieved a
    LEFT JOIN dwp_overall_mpl_expected e
        ON a.cpa = e.cpa 
       AND a."Date" = e."Date"
),

rolled AS (
    SELECT
        *,
        -- Rolling 3 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_3,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_3,

        -- Rolling 6 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_6,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_6,

        -- Rolling 12 months
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS JO_Expected_Rolling_12,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS JO_Actual_Rolling_12,

        -- Contract-to-date
        SUM(JO_Expected) OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JO_Expected_CTD,
        SUM(JO_Actual)   OVER (PARTITION BY cpa ORDER BY "Date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JO_Actual_CTD
    FROM base
),

performance AS (
    SELECT
        *,
        DIV0(JO_Actual_Rolling_3,  JO_Expected_Rolling_3)   AS Performance_Rolling_3,
        DIV0(JO_Actual_Rolling_6,  JO_Expected_Rolling_6)   AS Performance_Rolling_6,
        DIV0(JO_Actual_Rolling_12, JO_Expected_Rolling_12)  AS Performance_Rolling_12,
        DIV0(JO_Actual_CTD,        JO_Expected_CTD)         AS Performance_CTD
    FROM rolled
)

SELECT
    *,
    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance DESC
         )
    END AS Performance_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_3 DESC
         )
    END AS Performance_Rolling_3_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_6 DESC
         )
    END AS Performance_Rolling_6_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_Rolling_12 DESC
         )
    END AS Performance_Rolling_12_Rank,

    CASE WHEN cpa <> 'National'
         THEN RANK() OVER (
              PARTITION BY "Date" 
              ORDER BY Performance_CTD DESC
         )
    END AS Performance_CTD_Rank
FROM performance
);
```

</details>

4. **Visualisation in Power BI**  
   - Presentation tables are loaded into Power BI to create dashboards for SLT.  
   - Provides visual insights on AKG performance relative to other UK providers.

## Impact
- Fully automated MI Pack processing, eliminating manual steps and providing visuals  
- Faster and more reliable reporting for SLT  
- Standardised and cleaned data, ready for BI visualisation  
- Easy to scale and adapt for future reporting needs  
