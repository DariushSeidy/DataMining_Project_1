from clean_up_data import clean_up_data
from item_transaction_matrix import (
    create_item_transaction_matrix,
    analyze_matrix_properties,
)
import os
import logging

# Get the current directory and construct file path
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "Online Retail.xlsx")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=os.path.join(current_dir, "logs.log"),
)

# Step 1: Clean the data
logging.info("=== STEP 1: DATA CLEANING ===")
# cleaned_file_path = clean_up_data(file_path)
cleaned_file_path = os.path.join(current_dir, "Online Retail_Cleaned.xlsx")
logging.info(f"Cleaned data saved to: {cleaned_file_path}")

# Step 2: Create Item-Transaction Matrix
logging.info("=== STEP 2: CREATING ITEM-TRANSACTION MATRIX ===")
matrix, items_list, transactions_list, matrix_file_path = (
    create_item_transaction_matrix(cleaned_file_path)
)

if matrix is not None:
    # Analyze matrix properties
    logging.info("=== MATRIX ANALYSIS ===")
    analysis = analyze_matrix_properties(matrix)

    if analysis:
        logging.info(f"Matrix Shape: {analysis['matrix_shape']}")
        logging.info(f"Total Transactions: {analysis['total_transactions']}")
        logging.info(f"Total Items: {analysis['total_items']}")
        logging.info(f"Matrix Density: {analysis['matrix_density']:.2f}%")
        logging.info(
            f"Average Items per Transaction: {analysis['avg_items_per_transaction']:.2f}"
        )
        logging.info(
            f"Average Transactions per Item: {analysis['avg_transactions_per_item']:.2f}"
        )

        logging.info("Top 10 Most Popular Items:")
        for item, count in analysis["most_popular_items"].items():
            logging.info(f"  {item}: {count} transactions")

        logging.info("Top 10 Largest Transactions:")
        for transaction, count in analysis["largest_transactions"].items():
            logging.info(f"  Transaction {transaction}: {count} items")

    logging.info(f"Item-Transaction matrix saved to: {matrix_file_path}")
else:
    logging.error("Failed to create item-transaction matrix")
