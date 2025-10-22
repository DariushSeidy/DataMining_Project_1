import pandas as pd
import numpy as np
import os
import logging


def create_item_transaction_matrix(file_path):
    """
    Creates a binary item-transaction matrix from cleaned retail data.

    Args:
        file_path: Path to the cleaned Excel file

    Returns:
        tuple: (matrix_df, items_list, transactions_list)
    """

    try:
        # Read the cleaned data
        logging.info(f"Reading cleaned data from: {file_path}")
        df = pd.read_excel(file_path)

        logging.info(f"Loaded data shape: {df.shape}")

        # Create binary matrix
        # Pivot table: InvoiceNo (rows) vs StockCode (columns), values = 1 if item exists
        matrix = df.groupby(["InvoiceNo", "StockCode"]).size().unstack(fill_value=0)

        # Convert to binary (1 if item exists in transaction, 0 otherwise)
        matrix = (matrix > 0).astype(int)

        logging.info(f"Item-Transaction matrix shape: {matrix.shape}")
        logging.info(f"Number of unique transactions: {matrix.shape[0]}")
        logging.info(f"Number of unique items: {matrix.shape[1]}")

        # Get lists of items and transactions
        items_list = matrix.columns.tolist()
        transactions_list = matrix.index.tolist()

        # Save the matrix
        output_path = os.path.join(
            os.path.dirname(file_path), "Item_Transaction_Matrix.xlsx"
        )
        matrix.to_excel(output_path)

        logging.info(f"Item-Transaction matrix saved to: {output_path}")

        # Display some statistics
        logging.info(
            f"Matrix density: {(matrix.sum().sum() / (matrix.shape[0] * matrix.shape[1])) * 100:.2f}%"
        )
        logging.info(f"Average items per transaction: {matrix.sum(axis=1).mean():.2f}")
        logging.info(f"Average transactions per item: {matrix.sum(axis=0).mean():.2f}")

        return matrix, items_list, transactions_list, output_path

    except Exception as e:
        logging.error(f"Error creating item-transaction matrix: {str(e)}")
        return None, None, None, None


def analyze_matrix_properties(matrix):
    """
    Analyzes properties of the item-transaction matrix.

    Args:
        matrix: The binary item-transaction matrix

    Returns:
        dict: Analysis results
    """

    if matrix is None:
        return None

    analysis = {
        "matrix_shape": matrix.shape,
        "total_transactions": matrix.shape[0],
        "total_items": matrix.shape[1],
        "matrix_density": (matrix.sum().sum() / (matrix.shape[0] * matrix.shape[1]))
        * 100,
        "avg_items_per_transaction": matrix.sum(axis=1).mean(),
        "avg_transactions_per_item": matrix.sum(axis=0).mean(),
        "max_items_in_transaction": matrix.sum(axis=1).max(),
        "min_items_in_transaction": matrix.sum(axis=1).min(),
        "most_popular_items": matrix.sum(axis=0).nlargest(10).to_dict(),
        "largest_transactions": matrix.sum(axis=1).nlargest(10).to_dict(),
    }

    return analysis
