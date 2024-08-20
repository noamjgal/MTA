import pyarrow.parquet as pq
import pyarrow as pa

# File path
file_path = '/Users/noamgal/DSProjects/MTA/MTA_Subway_Origin-Destination_Ridership_Estimate__2023_20240727.parquet'

def inspect_parquet_file(file_path):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)

    # Get schema (column names and types)
    schema = parquet_file.schema_arrow
    print("Column names and types:")
    for field in schema:
        print(f"- {field.name}: {field.type}")

    # Read the first few rows
    print("\nFirst few rows:")
    first_rows = next(parquet_file.iter_batches(batch_size=5)).to_pandas()
    print(first_rows)

    # Find the timestamp column
    timestamp_col = next((col for col in schema.names if 'time' in col.lower() or 'date' in col.lower()), None)

    if timestamp_col:
        # Read only the timestamp column
        timestamp_data = pq.read_table(file_path, columns=[timestamp_col])
        
        # Convert to pandas and find min and max dates
        timestamps = timestamp_data.to_pandas()[timestamp_col]
        min_date = timestamps.min()
        max_date = timestamps.max()

        print(f"\nDate range for column '{timestamp_col}':")
        print(f"Earliest date: {min_date}")
        print(f"Latest date: {max_date}")
    else:
        print("\nNo timestamp column found. Please specify the correct column name for temporal analysis.")

    # Basic statistics for numeric columns
    print("\nBasic statistics for numeric columns:")
    for field in schema:
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            col_stats = pq.read_table(file_path, columns=[field.name]).to_pandas()[field.name].describe()
            print(f"\nStatistics for '{field.name}':")
            print(col_stats)

# Runs basic inspection of column data
inspect_parquet_file(file_path)