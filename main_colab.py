from clean_up_data import clean_up_data
from item_transaction_matrix import (
    create_item_transaction_matrix,
    analyze_matrix_properties,
)
import os
import logging
import sys

# Colab-specific setup
print("=== Data Mining Project - Item Transaction Matrix Creation ===")

# Get the current directory and construct file path
current_dir = os.getcwd()  # Use getcwd() for Colab instead of __file__
file_path = os.path.join(current_dir, "Online Retail.xlsx")

# Set up logging for Colab (console output instead of file)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # Output to console in Colab
)

# Check if files exist
if not os.path.exists(file_path):
    logging.error(f"Original file not found: {file_path}")
    logging.info("Available files:")
    for file in os.listdir(current_dir):
        logging.info(f"  - {file}")
    sys.exit(1)

# Step 1: Clean the data
logging.info("=== STEP 1: DATA CLEANING ===")
cleaned_file_path = clean_up_data(file_path)
if cleaned_file_path is None:
    logging.error("Data cleaning failed!")
    sys.exit(1)
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

    # Display first few rows of the matrix for verification
    logging.info("=== MATRIX PREVIEW ===")
    logging.info(f"First 5 rows and 10 columns of the matrix:")
    print(matrix.iloc[:5, :10])

else:
    logging.error("Failed to create item-transaction matrix")

print("=== Process completed! ===")
