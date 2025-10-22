from clean_up_data import clean_up_data
from item_transaction_matrix_optimized import (
    create_item_transaction_matrix_streaming,
    create_item_transaction_matrix_chunked,
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

# Step 2: Create Item-Transaction Matrix (Memory-Optimized)
logging.info("=== STEP 2: CREATING ITEM-TRANSACTION MATRIX (STREAMING) ===")

# Try streaming approach first (most memory efficient)
logging.info("Attempting streaming approach...")
items_list, transactions_list, matrix_file_path = (
    create_item_transaction_matrix_streaming(cleaned_file_path)
)

if items_list is not None:
    logging.info(f"✅ Streaming approach successful!")
    logging.info(
        f"Matrix dimensions: {len(transactions_list)} transactions × {len(items_list)} items"
    )
    logging.info(f"Item-Transaction matrix saved to: {matrix_file_path}")

    # Try chunked approach as backup for analysis
    logging.info("Creating chunked version for analysis...")
    matrix, _, _, chunked_file_path = create_item_transaction_matrix_chunked(
        cleaned_file_path
    )

    if matrix is not None:
        logging.info("=== MATRIX ANALYSIS ===")
        logging.info(f"Matrix Shape: {matrix.shape}")
        logging.info(f"Total Transactions: {matrix.shape[0]}")
        logging.info(f"Total Items: {matrix.shape[1]}")

        # Calculate density
        density = (matrix.sum().sum() / (matrix.shape[0] * matrix.shape[1])) * 100
        logging.info(f"Matrix Density: {density:.2f}%")
        logging.info(f"Average Items per Transaction: {matrix.sum(axis=1).mean():.2f}")
        logging.info(f"Average Transactions per Item: {matrix.sum(axis=0).mean():.2f}")

        # Show most popular items
        popular_items = matrix.sum(axis=0).nlargest(10)
        logging.info("Top 10 Most Popular Items:")
        for item, count in popular_items.items():
            logging.info(f"  {item}: {count} transactions")

        # Show largest transactions
        large_transactions = matrix.sum(axis=1).nlargest(10)
        logging.info("Top 10 Largest Transactions:")
        for transaction, count in large_transactions.items():
            logging.info(f"  Transaction {transaction}: {count} items")

        logging.info(f"Chunked matrix saved to: {chunked_file_path}")

        # Display first few rows
        logging.info("=== MATRIX PREVIEW ===")
        logging.info("First 5 rows and 10 columns:")
        print(matrix.iloc[:5, :10])

else:
    logging.error("❌ Streaming approach failed, trying chunked approach...")
    matrix, items_list, transactions_list, matrix_file_path = (
        create_item_transaction_matrix_chunked(cleaned_file_path)
    )

    if matrix is not None:
        logging.info("✅ Chunked approach successful!")
        logging.info(f"Matrix dimensions: {matrix.shape}")
        logging.info(f"Item-Transaction matrix saved to: {matrix_file_path}")
    else:
        logging.error("❌ Both approaches failed!")

print("=== Process completed! ===")
