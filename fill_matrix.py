import pandas as pd
import os
import gc
import csv


def fill_matrix():
    """
    Reads Online Retail_Cleaned.xlsx, groups by InvoiceNo,
    and fills matrix.csv with 1s where items exist in each invoice, 0s elsewhere.
    Uses CSV streaming for maximum memory efficiency with large datasets.
    Colab-friendly: uses getcwd() for path detection.
    """
    try:
        # Get the current directory (Colab-friendly)
        # Works in both regular Python and Google Colab
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # __file__ not available in Colab, use getcwd() instead
            current_dir = os.getcwd()

        # Path to the cleaned retail data
        data_path = os.path.join(current_dir, "Online Retail_Cleaned.xlsx")

        # Path to the matrix file (CSV format for memory efficiency)
        matrix_path = os.path.join(current_dir, "matrix.csv")

        print(f"Working directory: {current_dir}")
        print(f"Reading data from: {data_path}")

        # Check if files exist (helpful for Colab debugging)
        if not os.path.exists(data_path):
            print(f"Error: Data file not found at {data_path}")
            print("Available files in current directory:")
            for file in os.listdir(current_dir):
                if file.endswith((".xlsx", ".xls")):
                    print(f"  - {file}")
            return None

        if not os.path.exists(matrix_path):
            print(f"Error: Matrix file not found at {matrix_path}")
            print(
                "Make sure to run make_matrix.py first to create the matrix structure."
            )
            return None

        # Read the cleaned retail data
        df_data = pd.read_excel(data_path)
        print(f"Data shape: {df_data.shape}")

        # Read the CSV header to get column structure (memory-efficient)
        with open(matrix_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)

        items_columns = header[1:]  # Skip 'InvoiceNo' column
        print(
            f"Number of columns: {len(header)} (1 InvoiceNo + {len(items_columns)} items)"
        )

        # Get unique invoices
        unique_invoices = df_data["InvoiceNo"].unique()
        print(f"\nProcessing {len(unique_invoices)} unique invoices...")

        # Open CSV file in append mode for streaming writes
        # CSV streaming is much more memory-efficient than Excel
        # Use try-finally to ensure file is always closed
        csv_file = open(matrix_path, "a", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)

        try:
            # Process each invoice one at a time
            for idx, invoice_no in enumerate(unique_invoices, 1):
                # Get all items in this invoice
                invoice_group = df_data[df_data["InvoiceNo"] == invoice_no]
                items_in_invoice = set(invoice_group["Description"].astype(str))

                # Create row list starting with InvoiceNo
                row = [invoice_no]

                # Fill in 1s for items that exist in this invoice, 0s otherwise
                for item in items_columns:
                    row.append(1 if item in items_in_invoice else 0)

                # Write row immediately to CSV (streaming, no memory buildup)
                csv_writer.writerow(row)

                # Free up memory - delete temporary variables
                del invoice_group, items_in_invoice, row

                # Progress update
                if idx % 500 == 0:
                    print(
                        f"Processed and inserted {idx}/{len(unique_invoices)} invoices..."
                    )
                    # Flush buffer to disk periodically
                    csv_file.flush()
                    # Force garbage collection periodically
                    gc.collect()

            num_invoices = len(unique_invoices)
            print(f"\nCompleted processing {num_invoices} invoices")
        finally:
            # Always close CSV file, even if an error occurs
            csv_file.close()

        # Final memory cleanup
        del unique_invoices
        gc.collect()

        print(f"Matrix file updated at: {matrix_path}")
        print(f"Final shape: {num_invoices} rows x {len(items_columns) + 1} columns")

        return matrix_path

    except FileNotFoundError as e:
        print(f"Error: File not found - {str(e)}")
        return None
    except KeyError as e:
        print(f"Error: Column not found - {str(e)}")
        return None
    except Exception as e:
        print(f"Error filling matrix: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    fill_matrix()
