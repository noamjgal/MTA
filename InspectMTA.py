import pandas as pd
import fastparquet as fp
import numpy as np

# File path
file_path = '/Users/noamgal/DSProjects/MTA/MTA_Subway_Origin-Destination_Ridership_Estimate__2023_20240727.parquet'

def inspect_parquet_file(file_path):
    pf = fp.ParquetFile(file_path)
    
    # Initialize variables for aggregation
    total_rows = 0
    earliest_date = pd.Timestamp.max
    latest_date = pd.Timestamp.min
    numeric_data = []

    # Process the file in chunks
    for df in pf.iter_row_groups():
        total_rows += len(df)
        
        # Date processing
        df['Date'] = pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2) + '-01')
        earliest_date = min(earliest_date, df['Date'].min())
        latest_date = max(latest_date, df['Date'].max())
        
        # Collect numeric data for later analysis
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        numeric_data.append(df[numeric_cols])

    # Print basic information
    print(f"Total rows: {total_rows}")
    print(f"Columns: {', '.join(pf.columns)}")

    print(f"\nEarliest date: {earliest_date.strftime('%Y-%m-%d')}")
    print(f"Latest date: {latest_date.strftime('%Y-%m-%d')}")

    # Combine numeric data and calculate overall statistics
    all_numeric_data = pd.concat(numeric_data, ignore_index=True)
    overall_stats = all_numeric_data.agg(['count', 'mean', 'std', 'min', 'max'])
    
    print("\nOverall statistics for numeric columns:")
    print(overall_stats)

# Run the inspection
inspect_parquet_file(file_path)
