import json
import os
import random
import traceback
from multiprocessing import Pool

from faker import Faker

from config.config import load_config
from utils.logger import setup_logger

# Setup Logger
logger = setup_logger("generate_large_dataset")

# Load Configuration
config = load_config("config.yaml")
OUTPUT_DIR = os.path.abspath(config["output_directory"])
NUM_RECORDS = config["num_records"]
NUM_PARTITIONS = config["num_partitions"]
BATCH_LOG_INTERVAL = config["batch_log_interval"]
CHUNK_SIZE = config.get("chunk_size", 10000)  # Configure chunk size for writing data

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Initialize Faker
fake = Faker()


# Generate a single file with random data
def generate_file(partition_id, num_records, batch_log_interval, chunk_size):
    file_path = os.path.join(OUTPUT_DIR, f"data_part_{partition_id}.json")
    logger.info(f"🚀 Starting generation for {file_path}")

    try:
        data_chunk = []
        for i in range(num_records):
            record = {
                "id": str(fake.uuid4()),  # Ensure it's a string
                "category": random.choice(["A", "B", "C", "D"]),
                "value": round(random.uniform(10, 10000), 2),
                "timestamp": fake.date_time_this_decade().isoformat()
            }
            data_chunk.append(record)

            # Write data in chunks to file
            if (i + 1) % chunk_size == 0 or (i + 1) == num_records:
                with open(file_path, "a") as f:
                    for record in data_chunk:
                        f.write(json.dumps(record) + "\n")
                data_chunk.clear()  # Clear chunk after writing

            # Log progress every batch_log_interval records
            if (i + 1) % batch_log_interval == 0:
                logger.info(f"✅ {i + 1} records generated for {file_path}")

        logger.info(f"🎯 File {file_path} generated successfully with {num_records} records.")
    except Exception as e:
        logger.error(f"❌ Error generating file {file_path}: {e}\n{traceback.format_exc()}")

# Run multiprocessing for faster execution
def generate_dataset():
    logger.info("📌 Dataset generation started.")

    try:
        # Split total records per partition
        records_per_partition = NUM_RECORDS // NUM_PARTITIONS

        # Run multiprocessing for faster execution
        with Pool(NUM_PARTITIONS) as pool:
            pool.starmap(generate_file,
                         [(i, records_per_partition, BATCH_LOG_INTERVAL, CHUNK_SIZE) for i in range(NUM_PARTITIONS)])

        logger.info(f"✅ Dataset generation completed. Files saved in {OUTPUT_DIR}")
    except KeyboardInterrupt:
        logger.warning("⚠️ Process interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Dataset generation failed: {e}\n{traceback.format_exc()}")


# Main entry point
if __name__ == "__main__":
    generate_dataset()
