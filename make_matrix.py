import csv
import os


def create_matrix_excel():
    """
    Reads items from items.log and creates a CSV file with each item as a column.
    Saves the matrix CSV file in the same directory using os.path.
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

        # Path to items.log
        log_path = os.path.join(current_dir, "items.log")

        # Read items from log file
        with open(log_path, "r", encoding="utf-8") as f:
            items = [line.strip() for line in f if line.strip()]

        print(f"Found {len(items)} items in items.log")

        # Create column names with "InvoiceNo" as first column, then items
        columns = ["InvoiceNo"] + items

        # Create the output file path (CSV instead of Excel)
        output_path = os.path.join(current_dir, "matrix.csv")

        # Write CSV header only (empty file with headers)
        # CSV is much more memory-efficient than Excel for large files
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)

        print(f"Matrix CSV file created at: {output_path}")
        print(
            f"Number of columns: {len(columns)} (1 InvoiceNo column + {len(items)} item columns)"
        )
        print(
            "Note: CSV format is more memory-efficient for large datasets than Excel."
        )

        return output_path

    except FileNotFoundError as e:
        print(f"Error: File not found - {str(e)}")
        return None
    except Exception as e:
        print(f"Error creating matrix: {str(e)}")
        return None


if __name__ == "__main__":
    create_matrix_excel()
