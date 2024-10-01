import pandas as pd
import fastparquet as fp

# File path
file_path = '/Users/noamgal/DSProjects/MTA/MTA_Subway_Origin-Destination_Ridership_Estimate__2023_20240727.parquet'

def inspect_parquet_file(file_path):
    # Read the entire Parquet file
    df = fp.ParquetFile(file_path).to_pandas()

    # Print basic information
    print(f"Total rows: {len(df)}")
    print(f"Columns: {', '.join(df.columns)}")

    # Find the earliest and latest dates
    df['Date'] = pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'].astype(str).str.zfill(2) + '-01')
    earliest_date = df['Date'].min()
    latest_date = df['Date'].max()

    print(f"\nEarliest date: {earliest_date.strftime('%Y-%m-%d')}")
    print(f"Latest date: {latest_date.strftime('%Y-%m-%d')}")

    # Overall statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    overall_stats = df[numeric_cols].describe()
    
    print("\nOverall statistics for numeric columns:")
    print(overall_stats)

# Run the inspection
inspect_parquet_file(file_path)