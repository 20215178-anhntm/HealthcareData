import argparse, sys
from src.ingestion.csv_ingestor import CsvIngestor
from src.processing.bronze_to_silver import BronzeToSilver
from src.processing.silver_to_gold import SilverToGold
from src.common.logging import setup_logger
from typing import Union

log = setup_logger("batch_etl")

def main(mode: str, since: Union[str, None] = None):
    try:
        ing = CsvIngestor(since=since)
        if mode in ("all", "bronze"):
            ing.ingest_patients()
            ing.ingest_insurance()
            ing.ingest_diabetes()
            ing.ingest_noshows()
        if mode in ("all", "silver"):
            BronzeToSilver().run()
        if mode in ("all", "gold"):
            SilverToGold().run()
        log.info("batch_etl completed successfully")
    except Exception as e:
        log.exception(f"batch_etl failed: {e}")
        sys.exit(2)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=["all","bronze","silver","gold"])
    ap.add_argument("--since", default=None, help="YYYY-MM-DD: ingest raw with LastModified >= since")
    args = ap.parse_args()
    main(args.mode, args.since)
