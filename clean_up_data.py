import pandas as pd
import os
import logging


def clean_up_data(file_path):
    """
    Opens the given file and removes incomplete or invalid records.
    Deletes rows where:
    - InvoiceNo, StockCode, Quantity, InvoiceDate, CustomerID are empty
    - UnitPrice is <= 0 or empty

    Prints each deleted row for tracking.
    """

    try:
        # Read the Excel file
        logging.info(f"Opening file: {file_path}")
        df = pd.read_excel(file_path)

        logging.info(f"Original dataset shape: {df.shape}")
        logging.info(f"Original number of rows: {len(df)}")

        # Create a copy to track deletions
        original_df = df.copy()

        # Track deleted rows
        deleted_rows = []

        # Check for rows to delete
        for index, row in df.iterrows():
            should_delete = False
            deletion_reason = []

            # Check if required columns are empty
            required_columns = [
                "InvoiceNo",
                "StockCode",
                "Quantity",
                "InvoiceDate",
                "CustomerID",
            ]
            for col in required_columns:
                if pd.isna(row[col]) or (
                    isinstance(row[col], str) and row[col].strip() == ""
                ):
                    should_delete = True
                    deletion_reason.append(f"{col} is empty")

            # Check UnitPrice conditions
            if pd.isna(row["UnitPrice"]) or row["UnitPrice"] <= 0:
                should_delete = True
                deletion_reason.append(
                    f"UnitPrice is {'empty' if pd.isna(row['UnitPrice']) else '<= 0'}"
                )

            # If row should be deleted, add to deleted_rows list
            if should_delete:
                deleted_row = row.to_dict()
                deleted_row["deletion_reason"] = "; ".join(deletion_reason)
                deleted_rows.append((index, deleted_row))

        # Print deleted rows
        logging.info(f"\n=== DELETED ROWS ({len(deleted_rows)} total) ===")
        for row_index, deleted_row in deleted_rows:
            logging.info(
                f"\nRow {row_index + 2} (Excel row {row_index + 2}):"
            )  # +2 because Excel is 1-indexed and has header
            logging.info(f"  Reason: {deleted_row['deletion_reason']}")
            logging.info(f"  Data: {deleted_row}")

        # Remove the problematic rows
        rows_to_drop = [row_index for row_index, _ in deleted_rows]
        df_cleaned = df.drop(rows_to_drop)

        logging.info(f"\n=== CLEANUP SUMMARY ===")
        logging.info(f"Original rows: {len(df)}")
        logging.info(f"Deleted rows: {len(deleted_rows)}")
        logging.info(f"Remaining rows: {len(df_cleaned)}")
        logging.info(f"Data reduction: {len(deleted_rows)/len(df)*100:.2f}%")

        output_path = os.path.join(os.path.dirname(file_path), "Online Retail_Cleaned.xlsx")
        df_cleaned.to_excel(output_path, index=False)

        return output_path

    except FileNotFoundError:
        logging.error(f"Error: File '{file_path}' not found")
        return None
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        return None


