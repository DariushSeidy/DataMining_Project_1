import pandas as pd
import os
import gc
from datetime import datetime


def create_monthly_batches():
    """
    Reads Online Retail_Cleaned.xlsx and creates monthly batch CSV files.
    Each batch file contains all transactions for one month, grouped by InvoiceDate.
    These batches can then be processed sequentially to fill matrix.csv efficiently.

    Colab-friendly: uses getcwd() for path detection.

    Returns:
        dict: Mapping of month strings to batch file paths
    """
    try:
        # Get the current directory (Colab-friendly)
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # __file__ not available in Colab, use getcwd() instead
            current_dir = os.getcwd()

        # Path to the cleaned retail data
        data_path = os.path.join(current_dir, "Online Retail_Cleaned.xlsx")

        # Create batches directory
        batches_dir = os.path.join(current_dir, "batches")
        os.makedirs(batches_dir, exist_ok=True)

        print(f"Working directory: {current_dir}")
        print(f"Reading data from: {data_path}")

        # Check if file exists
        if not os.path.exists(data_path):
            print(f"Error: Data file not found at {data_path}")
            print("Available files in current directory:")
            for file in os.listdir(current_dir):
                if file.endswith((".xlsx", ".xls")):
                    print(f"  - {file}")
            return None

        # Read the cleaned retail data
        print("Loading data... (this may take a moment for large files)")
        df_data = pd.read_excel(data_path)
        print(f"Data shape: {df_data.shape}")
        print(f"Total rows: {len(df_data):,}")

        # Ensure InvoiceDate is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_data["InvoiceDate"]):
            df_data["InvoiceDate"] = pd.to_datetime(df_data["InvoiceDate"])

        print("\nCreating monthly batches...")

        # Create year-month column for grouping
        df_data["YearMonth"] = df_data["InvoiceDate"].dt.to_period("M")

        # Get unique year-month periods
        unique_months = sorted(df_data["YearMonth"].unique())
        print(f"Found {len(unique_months)} unique months")
        print(f"Date range: {unique_months[0]} to {unique_months[-1]}")

        batch_files = {}

        # Create a batch file for each month
        for month_period in unique_months:
            # Filter data for this month
            month_data = df_data[df_data["YearMonth"] == month_period]

            # Convert period to string format (e.g., "2010-12")
            month_str = str(month_period)

            # Create batch filename
            batch_filename = f"batch_{month_str}.csv"
            batch_path = os.path.join(batches_dir, batch_filename)

            # Save to CSV (don't include the YearMonth column we added)
            month_data_to_save = month_data.drop(columns=["YearMonth"])
            month_data_to_save.to_csv(batch_path, index=False)

            # Store batch info
            num_rows = len(month_data_to_save)
            num_invoices = month_data_to_save["InvoiceNo"].nunique()

            batch_files[month_str] = {
                "path": batch_path,
                "rows": num_rows,
                "invoices": num_invoices,
                "date_range": (
                    month_data_to_save["InvoiceDate"].min(),
                    month_data_to_save["InvoiceDate"].max(),
                ),
            }

            print(
                f"  Created {batch_filename}: {num_rows:,} rows, {num_invoices:,} unique invoices"
            )

            # Free memory
            del month_data, month_data_to_save
            gc.collect()

        # Summary
        print("\n=== BATCH CREATION SUMMARY ===")
        print(f"Total batches created: {len(batch_files)}")
        print(f"Batches directory: {batches_dir}")
        print("\nBatch files:")
        for month, info in batch_files.items():
            print(f"  {month}: {info['invoices']:,} invoices, {info['rows']:,} rows")

        # Clean up
        del df_data
        gc.collect()

        return batch_files

    except FileNotFoundError as e:
        print(f"Error: File not found - {str(e)}")
        return None
    except KeyError as e:
        print(f"Error: Column not found - {str(e)}")
        print("Make sure the data file has 'InvoiceDate' column.")
        return None
    except Exception as e:
        print(f"Error creating batches: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


def get_batch_files_list(batches_dir=None):
    """
    Returns a sorted list of batch file paths in chronological order.

    Args:
        batches_dir: Directory containing batch files. If None, auto-detects.

    Returns:
        list: Sorted list of batch file paths
    """
    try:
        if batches_dir is None:
            try:
                current_dir = os.path.dirname(os.path.abspath(__file__))
            except NameError:
                current_dir = os.getcwd()
            batches_dir = os.path.join(current_dir, "batches")

        if not os.path.exists(batches_dir):
            print(f"Batches directory not found: {batches_dir}")
            return []

        # Get all batch CSV files
        batch_files = [
            f
            for f in os.listdir(batches_dir)
            if f.startswith("batch_") and f.endswith(".csv")
        ]

        # Sort by filename (which contains date)
        batch_files.sort()

        # Return full paths
        return [os.path.join(batches_dir, f) for f in batch_files]

    except Exception as e:
        print(f"Error getting batch files: {str(e)}")
        return []


if __name__ == "__main__":
    print("=== CREATING MONTHLY BATCHES ===")
    batch_files = create_monthly_batches()

    if batch_files:
        print("\n✅ Batch files created successfully!")
        print("\nNext steps:")
        print("1. Process these batches sequentially to fill matrix.csv")
        print("2. Each batch can be processed one at a time for memory efficiency")
        print("3. Use the batch files for streaming simulation")
    else:
        print("\n❌ Failed to create batch files")
