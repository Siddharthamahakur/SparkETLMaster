import json
import os
import random
import traceback
from multiprocessing import Pool, cpu_count
from faker import Faker
from config.config import load_config
from utils.logger import setup_logger
import time
import gzip

# ✅ Setup Logger
logger = setup_logger("generate_large_dataset")

# ✅ Load Configuration
config = load_config("config.yaml")
OUTPUT_DIR = os.path.abspath(config["dataset"]["output_directory"])
NUM_RECORDS = config["etl"]["num_records"]
NUM_PARTITIONS = config["etl"]["num_partitions"]
BATCH_LOG_INTERVAL = config["etl"]["batch_log_interval"]
CHUNK_SIZE = config["etl"].get("chunk_size", 10000)

# ✅ Ensure Output Directory Exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ Initialize Faker
fake = Faker()


# 🚀 Optimized Function to Generate a Single File
def generate_file(partition_id, num_records, batch_log_interval, chunk_size):
    """ Generate a single JSON file with random data efficiently """
    file_path = os.path.join(OUTPUT_DIR, f"data_part_{partition_id}.json.gz")
    logger.info(f"🚀 Starting generation for {file_path}")

    try:
        start_time = time.time()
        data_chunk = []
        record_count = 0

        with gzip.open(file_path, "wt", compresslevel=5, encoding="utf-8") as f:  # Compressed output
            for i in range(num_records):
                record = {
                    "id": str(fake.uuid4()),
                    "category": random.choice(["A", "B", "C", "D"]),
                    "value": round(random.uniform(10, 10000), 2),
                    "timestamp": fake.date_time_this_decade().isoformat()
                }
                data_chunk.append(record)
                record_count += 1

                # Write in batches
                if len(data_chunk) >= chunk_size or record_count == num_records:
                    f.write("\n".join(json.dumps(r) for r in data_chunk) + "\n")
                    data_chunk.clear()

                # Log progress
                if record_count % batch_log_interval == 0:
                    logger.info(f"✅ {record_count}/{num_records} records written in {file_path}")

        logger.info(
            f"🎯 File {file_path} generated with {num_records} records in {time.time() - start_time:.2f} seconds."
        )

    except Exception as e:
        logger.error(f"❌ Error generating file {file_path}: {e}\n{traceback.format_exc()}")


# 🚀 Multiprocessing for Faster Execution
def generate_dataset():
    """ Run multiprocessing for faster ETL dataset generation """
    logger.info("📌 Dataset generation started.")
    start_time = time.time()

    try:
        records_per_partition = NUM_RECORDS // NUM_PARTITIONS
        num_processes = min(NUM_PARTITIONS, cpu_count())

        with Pool(num_processes) as pool:
            pool.starmap(
                generate_file,
                [(i, records_per_partition, BATCH_LOG_INTERVAL, CHUNK_SIZE) for i in range(NUM_PARTITIONS)]
            )

        logger.info(f"✅ Dataset generation completed in {time.time() - start_time:.2f} seconds.")
        logger.info(f"📁 Files saved in {OUTPUT_DIR}")
    except KeyboardInterrupt:
        logger.warning("⚠️ Process interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Dataset generation failed: {e}\n{traceback.format_exc()}")


# ✅ Main Entry Point
if __name__ == "__main__":
    generate_dataset()
