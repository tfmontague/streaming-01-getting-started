"""
Batch Process C: Third transformation

Read from a file, transform, write to a new file.
In this case, covert degree K to degree F.

Note: 
This is a batch process, but the file objects we use are 
often called 'file-like objects' or 'streams'.
Streaming differs in that the input data is unbounded.

Use logging, very helpful when working with batch and streaming processes. 

"""

# Import from Python Standard Library

import csv
import logging

# Set up basic configuration for logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Declare program constants
INPUT_FILE_NAME = "batchfile_2_kelvin.csv"
OUTPUT_FILE_NAME = "batchfile_3_farenheit.csv"

def convert_k_to_f(temp_k):
    """
    Convert temperature from Kelvin to Fahrenheit.

    Args:
    temp_k (float): Temperature in Kelvin.

    Returns:
    float: Temperature in Fahrenheit.
    """
    try:
        temp_k = float(temp_k)
        temp_f = (temp_k - 273.15) * 9/5 + 32
        return temp_f
    except ValueError:
        logging.error(f"Invalid input {temp_k} which is not a float.")
        return None

def process_rows(input_file_name, output_file_name):
    """
    Read temperature data in Kelvin from an input file,
    convert each temperature to Fahrenheit, and write to an output file.

    Args:
    input_file_name (str): Path to the input CSV file.
    output_file_name (str): Path to the output CSV file.
    """
    try:
        with open(input_file_name, mode='r', newline='') as infile:
            reader = csv.reader(infile)
            with open(output_file_name, mode='w', newline='') as outfile:
                writer = csv.writer(outfile)
                for row in reader:
                    if row:  # Ensure the row is not empty
                        temp_f = convert_k_to_f(row[0])
                        if temp_f is not None:
                            writer.writerow([temp_f])
    except FileNotFoundError:
        logging.error(f"File {input_file_name} not found.")
    except IOError:
        logging.error("An I/O error occurred.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

# Main execution block
if __name__ == "__main__":
    try:
        logging.info("===============================================")
        logging.info("Starting batch process C.")
        process_rows(INPUT_FILE_NAME, OUTPUT_FILE_NAME)
        logging.info("Processing complete! Check for new file.")
        logging.info("===============================================")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
