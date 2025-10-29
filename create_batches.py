import pandas as pd
import os
import gc
import csv
from collections import defaultdict


def create_monthly_batches():
    """
    Creates monthly batch CSV files from matrix.csv.

    Steps:
    1. Reads Online Retail_Cleaned.xlsx to get InvoiceNo -> Month mapping
    2. Reads matrix.csv row by row (streaming for efficiency)
    3. For each matrix row, determines its month and writes to appropriate batch file

    Each batch file contains the binary matrix rows (InvoiceNo + binary columns)
    for invoices from that specific month.

    Colab-friendly: uses getcwd() for path detection.

    Returns:
        dict: Mapping of month strings to batch file info
    """
    try:
        # Get the current directory (Colab-friendly)
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # __file__ not available in Colab, use getcwd() instead
            current_dir = os.getcwd()

        # Paths
        data_path = os.path.join(current_dir, "Online Retail_Cleaned.xlsx")
        matrix_path = os.path.join(current_dir, "matrix.csv")

        # Create batches directory
        batches_dir = os.path.join(current_dir, "batches")
        os.makedirs(batches_dir, exist_ok=True)

        print(f"Working directory: {current_dir}")
        print(f"Reading invoice dates from: {data_path}")
        print(f"Reading binary matrix from: {matrix_path}")

        # Check if files exist
        if not os.path.exists(data_path):
            print(f"Error: Data file not found at {data_path}")
            return None

        if not os.path.exists(matrix_path):
            print(f"Error: Matrix file not found at {matrix_path}")
            print("Make sure to run fill_matrix.py first to create matrix.csv")
            return None

        # Step 1: Read Online Retail_Cleaned.xlsx to create InvoiceNo -> Month mapping
        print("\n=== STEP 1: Creating InvoiceNo to Month mapping ===")
        print("Loading invoice dates...")

        # Read only InvoiceNo and InvoiceDate columns for efficiency
        df_data = pd.read_excel(data_path, usecols=["InvoiceNo", "InvoiceDate"])
        print(f"Loaded {len(df_data):,} transaction rows")

        # Ensure InvoiceDate is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_data["InvoiceDate"]):
            df_data["InvoiceDate"] = pd.to_datetime(df_data["InvoiceDate"])

        # Get earliest InvoiceDate for each InvoiceNo (an invoice can have multiple rows)
        invoice_dates = df_data.groupby("InvoiceNo")["InvoiceDate"].min()

        # Convert to year-month period (e.g., "2010-12")
        # IMPORTANT: Convert InvoiceNo to string for consistent matching with CSV
        invoice_to_month = {}
        for invoice_no, invoice_date in invoice_dates.items():
            month_period = invoice_date.to_period("M")
            # Convert InvoiceNo to string and strip whitespace to match CSV format
            # CSV always reads as string, so we need to match that format
            invoice_no_str = str(invoice_no).strip()
            invoice_to_month[invoice_no_str] = str(month_period)

        # Get unique months and sort
        unique_months = sorted(set(invoice_to_month.values()))
        print(f"Found {len(unique_months)} unique months")
        print(f"Date range: {unique_months[0]} to {unique_months[-1]}")
        print(f"Total invoices in mapping: {len(invoice_to_month):,}")

        # Debug: Show sample InvoiceNo types (first few)
        sample_invoices = list(invoice_to_month.keys())[:3]
        print(
            f"Sample InvoiceNo types in mapping (should be strings): {[type(inv).__name__ for inv in sample_invoices]}"
        )

        # Group InvoiceNos by month (InvoiceNos already converted to strings above)
        month_to_invoices = defaultdict(set)
        for invoice_no, month in invoice_to_month.items():
            month_to_invoices[month].add(invoice_no)  # invoice_no is already string

        # Free memory
        del df_data, invoice_dates
        gc.collect()

        # Step 2: Open batch CSV files for writing (one per month)
        print("\n=== STEP 2: Preparing batch CSV files ===")
        batch_files = {}
        batch_writers = {}

        # Read matrix header first to get structure
        with open(matrix_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # Read header row

        print(f"Matrix header: {len(header)} columns")

        # Create batch file for each month
        for month in unique_months:
            batch_filename = f"batch_{month}.csv"
            batch_path = os.path.join(batches_dir, batch_filename)

            # Open file and write header
            batch_file = open(batch_path, "w", newline="", encoding="utf-8")
            batch_writer = csv.writer(batch_file)
            batch_writer.writerow(header)  # Write same header as matrix.csv

            batch_files[month] = {
                "path": batch_path,
                "file": batch_file,
                "writer": batch_writer,
                "row_count": 0,
            }

            print(f"  Created {batch_filename}")

        print(f"\n=== STEP 3: Processing matrix.csv and writing to batches ===")

        # Step 3: Read matrix.csv row by row and route to appropriate batch file
        total_rows = 0
        matched_rows = 0
        unmatched_invoices = set()

        print(f"Starting to process matrix.csv rows...")
        print(f"InvoiceNo lookup dictionary size: {len(invoice_to_month):,} invoices")

        with open(matrix_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header (already processed)

            for row_idx, row in enumerate(reader, 1):
                if not row:  # Skip empty rows
                    continue

                total_rows += 1
                invoice_no = row[
                    0
                ]  # First column is InvoiceNo (already string from CSV)

                # Ensure InvoiceNo is string for matching (should already be, but be safe)
                invoice_no = str(invoice_no).strip() if invoice_no else None

                # Find which month this invoice belongs to
                month = invoice_to_month.get(invoice_no) if invoice_no else None

                if month and month in batch_files:
                    # Write row to appropriate batch file
                    batch_files[month]["writer"].writerow(row)
                    batch_files[month]["row_count"] += 1
                    matched_rows += 1
                else:
                    unmatched_invoices.add(invoice_no)

                # Progress update
                if row_idx % 1000 == 0:
                    print(
                        f"  Processed {row_idx:,} rows, matched {matched_rows:,} rows..."
                    )
                    gc.collect()

        # Close all batch files
        print("\nClosing batch files...")
        for month, info in batch_files.items():
            info["file"].close()
            # Remove file references (keep path and row_count)
            batch_files[month] = {
                "path": info["path"],
                "row_count": info["row_count"],
                "invoices": len(month_to_invoices.get(month, set())),
            }

        # Final summary
        print("\n=== BATCH CREATION SUMMARY ===")
        print(f"Total rows processed from matrix.csv: {total_rows:,}")
        print(f"Rows matched to batches: {matched_rows:,}")
        if unmatched_invoices:
            print(
                f"Warning: {len(unmatched_invoices)} invoices in matrix.csv not found in date mapping"
            )

        print(f"\nTotal batches created: {len(batch_files)}")
        print(f"Batches directory: {batches_dir}")
        print("\nBatch files:")
        for month in sorted(batch_files.keys()):
            info = batch_files[month]
            print(
                f"  {month}: {info['row_count']:,} rows, {info['invoices']:,} invoices"
            )

        # Clean up
        del invoice_to_month, month_to_invoices
        gc.collect()

        return batch_files

    except FileNotFoundError as e:
        print(f"Error: File not found - {str(e)}")
        return None
    except KeyError as e:
        print(f"Error: Column not found - {str(e)}")
        print("Make sure the data file has 'InvoiceNo' and 'InvoiceDate' columns.")
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
    print("=== CREATING MONTHLY MATRIX BATCHES ===")
    print("This will create binary matrix batches grouped by month.")
    print("Make sure matrix.csv exists (run fill_matrix.py first)\n")

    batch_files = create_monthly_batches()

    if batch_files:
        print("\n✅ Batch files created successfully!")
        print("\nNext steps for streaming simulation:")
        print("1. Process batch files sequentially in chronological order")
        print("2. Each batch contains the binary matrix rows for that month")
        print("3. Monitor matrix evolution as you process each batch")
    else:
        print("\n❌ Failed to create batch files")
