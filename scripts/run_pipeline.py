import logging
import os

# Configure logging before importing anything else
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

from etl.pipeline import run_etl

def main():
    run_etl()

if __name__ == "__main__":
    main()