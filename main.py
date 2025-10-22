from clean_up_data import clean_up_data
import os
import logging

# Get the current directory and construct file path
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "Online Retail.xlsx")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    filename=os.path.join(current_dir, 'logs.log')
)

output_path = clean_up_data(file_path)
logging.info(f"Cleaned data saved to: {output_path}")