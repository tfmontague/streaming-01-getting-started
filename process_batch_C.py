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
import csv
import logging

# Set up basic configuration for logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Declare program constants
INPUT_FILE_NAME = "batchfile_2_kelvin.csv"
OUTPUT_FILE_NAME = "batchfile_3_farenheit.csv"

def convert_k_to_f(temp_k):
    """Attempt to convert Kelvin to Fahrenheit, logging input."""
    try:
        temp_k = float(temp_k)
        fahrenheit = round((temp_k - 273.15) * 9/5 + 32, 2)
        return fahrenheit
    except ValueError:
        logging.error(f"Failed to convert {temp_k}; invalid input which is not a float.")
        return None

def process_rows(input_file_name, output_file_name):
    """Read and convert data, with added checks and logging."""
    logging.info("Starting processing rows.")

    with open(input_file_name, "r") as input_file:
        reader = csv.reader(input_file, delimiter=",")
        header = next(reader)  # Skip header
        logging.debug(f"Header: {header}")

        with open(output_file_name, "w", newline="") as output_file:
            writer = csv.writer(output_file, delimiter=",")
            writer.writerow(header)  # Write header to output

            for index, row in enumerate(reader):
                if row:  # Ensure row is not empty
                    logging.debug(f"Processing row {index + 1}: {row}")
                    try:
                        Year, Month, Day, Time, TempK = row
                        TempF = convert_k_to_f(TempK)
                        if TempF is not None:
                            writer.writerow([Year, Month, Day, Time, TempF])
                        else:
                            logging.warning(f"Skipped row {index + 1} due to invalid temperature data.")
                    except Exception as e:
                        logging.error(f"Error processing row {index + 1}: {e}")

if __name__ == "__main__":
    logging.info("===============================================")
    logging.info("Starting batch process C.")
    process_rows(INPUT_FILE_NAME, OUTPUT_FILE_NAME)
    logging.info("Processing complete! Check for new file.")
    logging.info("===============================================")

