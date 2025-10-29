import pandas as pd
import os
import logging


def extract_unique_descriptions():
    """
    Opens the cleaned Excel file and extracts unique descriptions from the Description column.
    Adds each description to a set and logs the set to the log file.
    """
    try:
        # Construct the file path using os.path
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "Online Retail_Cleaned.xlsx"
        )

        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "items.log")

        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            filename=log_path,
        )


        # Read the Excel file
        df = pd.read_excel(file_path)


        # Create a set to store unique descriptions
        unique_descriptions = set()

        # Iterate through each row
        for index, row in df.iterrows():
            description = row["Description"]

            # Add to set (handles duplicates automatically)
            if pd.notna(description) and str(description).strip():
                unique_descriptions.add(str(description).strip())

        # Log the set to the log file

        # Sort for easier reading
        sorted_descriptions = sorted(unique_descriptions)

        for desc in sorted_descriptions:
            logging.info(desc)


        return unique_descriptions

    except FileNotFoundError:
        logging.error(f"Error: File '{file_path}' not found")
        return None
    except KeyError as e:
        logging.error(f"Error: Column not found - {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error extracting descriptions: {str(e)}")
        return None


if __name__ == "__main__":
    extract_unique_descriptions()
