import json
import os
import random
import traceback
from multiprocessing import Pool
from faker import Faker
from utils.logger import setup_logger

# Setup Logger
logger = setup_logger("generate_large_dataset")

# Constants
fake = Faker()
OUTPUT_DIR = os.path.abspath("data/raw-data/")
NUM_RECORDS = 1_000_000  # Adjust based on file size needed
NUM_PARTITIONS = 10  # Parallel writes for faster generation
BATCH_LOG_INTERVAL = 100_000  # Log progress every 100,000 records

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Schema fields (for reference)
SCHEMA_FIELDS = [
    {"name": "id", "type": "string"},
    {"name": "category", "type": "string"},
    {"name": "value", "type": "double"},
    {"name": "timestamp", "type": "string"}
]


# Generate a single file with random data
def generate_file(partition_id):
    file_path = os.path.join(OUTPUT_DIR, f"data_part_{partition_id}.json")
    logger.info(f"🚀 Starting generation for {file_path}")

    try:
        with open(file_path, "w") as f:
            for i in range(NUM_RECORDS // NUM_PARTITIONS):
                record = {
                    "id": str(fake.uuid4()),  # Ensure it's a string
                    "category": random.choice(["A", "B", "C", "D"]),
                    "value": round(random.uniform(10, 10000), 2),
                    "timestamp": fake.date_time_this_decade().isoformat()
                }
                f.write(json.dumps(record) + "\n")

                # Log progress every BATCH_LOG_INTERVAL records
                if (i + 1) % BATCH_LOG_INTERVAL == 0:
                    logger.info(f"✅ {i + 1} records written in {file_path}")

        logger.info(f"🎯 File {file_path} generated successfully with {NUM_RECORDS // NUM_PARTITIONS} records.")
    except Exception as e:
        logger.error(f"❌ Error generating file {file_path}: {e}\n{traceback.format_exc()}")


# Run multiprocessing for faster execution
if __name__ == "__main__":
    logger.info("📌 Dataset generation started.")

    try:
        with Pool(NUM_PARTITIONS) as p:
            p.map(generate_file, range(NUM_PARTITIONS))

        logger.info(f"✅ Dataset generation completed. Files saved in {OUTPUT_DIR}")
    except KeyboardInterrupt:
        logger.warning("⚠️ Process interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Dataset generation failed: {e}\n{traceback.format_exc()}")
