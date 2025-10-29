import pandas as pd
import os


def create_matrix_excel():
    """
    Reads items from items.log and creates an Excel file with each item as a column.
    Saves the matrix Excel file in the same directory using os.path.
    """
    try:
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Path to items.log
        log_path = os.path.join(current_dir, "items.log")

        # Read items from log file
        with open(log_path, "r", encoding="utf-8") as f:
            items = [line.strip() for line in f if line.strip()]

        print(f"Found {len(items)} items in items.log")

        # Create a DataFrame with "InvoiceNo" as first column, then items
        # This shifts all items one column to the right
        columns = ["InvoiceNo"] + items
        df = pd.DataFrame(columns=columns)

        # Create the output file path
        output_path = os.path.join(current_dir, "matrix.xlsx")

        # Save to Excel
        df.to_excel(output_path, index=False)

        print(f"Matrix Excel file created at: {output_path}")
        print(
            f"Number of columns: {len(columns)} (1 InvoiceNo column + {len(items)} item columns)"
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
