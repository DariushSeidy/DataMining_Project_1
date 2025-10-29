import pandas as pd
import os
import gc
from openpyxl import load_workbook


def fill_matrix():
    """
    Reads Online Retail_Cleaned.xlsx, groups by InvoiceNo,
    and fills matrix.xlsx with 1s where items exist in each invoice, 0s elsewhere.
    Writes each invoice row immediately to avoid memory issues.
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

        # Path to the matrix file
        matrix_path = os.path.join(current_dir, "matrix.xlsx")

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

        # Read the existing matrix file to get the column structure
        df_matrix = pd.read_excel(matrix_path)
        print(f"Matrix shape: {df_matrix.shape}")
        print(f"Number of columns: {len(df_matrix.columns)}")

        # Get all column names (first is InvoiceNo, rest are items)
        items_columns = df_matrix.columns.tolist()[1:]  # Skip 'InvoiceNo' column

        # Free up memory - no longer need df_matrix
        del df_matrix
        gc.collect()

        # Get unique invoices
        unique_invoices = df_data["InvoiceNo"].unique()
        print(f"\nProcessing {len(unique_invoices)} unique invoices...")

        # Load the existing matrix workbook
        wb = load_workbook(matrix_path)
        ws = wb.active

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

            # Append this row to the worksheet
            ws.append(row)

            # Free up memory - delete temporary variables
            del invoice_group, items_in_invoice, row

            # Progress update
            if idx % 500 == 0:
                print(
                    f"Processed and inserted {idx}/{len(unique_invoices)} invoices..."
                )
                # Save periodically to avoid losing progress
                wb.save(matrix_path)
                # Force garbage collection periodically
                gc.collect()

        num_invoices = len(unique_invoices)
        print(f"\nCompleted processing {num_invoices} invoices")

        # Final save
        wb.save(matrix_path)

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
