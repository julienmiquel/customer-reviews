from google.cloud import bigquery
from typing import List, Dict, Any
from .config import PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_PLACE_DETAILS, BIGQUERY_TABLE_REVIEWS
from .logger import setup_logger

logger = setup_logger(__name__)

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        logger.info("BigQueryClient initialized")

    def get_existing_place_ids(self) -> List[str]:
        """Retrieve existing place IDs from BigQuery table."""
        logger.info(f"Fetching existing place IDs from table: {BIGQUERY_TABLE_PLACE_DETAILS}")
        try:
            query = f"""
                SELECT DISTINCT place_id
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_PLACE_DETAILS}`
            """
            query_job = self.client.query(query)
            results = query_job.result()
            existing_ids = [row.place_id for row in results]
            logger.info(f"Found {len(existing_ids)} existing place IDs")
            return existing_ids
        except Exception as e:
            logger.error(f"Error fetching existing place IDs: {str(e)}", exc_info=True)
            raise

    def save_reviews(self, reviews: List[Dict[str, Any]]) -> None:
        """Save reviews to BigQuery table."""
        logger.info(f"Saving {len(reviews)} reviews to table: {BIGQUERY_TABLE_REVIEWS}")
        try:
            table_ref = self.client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TABLE_REVIEWS)
            
            schema = [
                bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("overall_rating", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("total_ratings", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("reviews", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("review_rating", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("time_review", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
                ])
            ]

            try:
                table = self.client.get_table(table_ref)
                logger.info(f"Table {BIGQUERY_TABLE_REVIEWS} already exists")
            except Exception as e:
                if "Not found" in str(e):
                    table = bigquery.Table(table_ref, schema=schema)
                    table = self.client.create_table(table)
                    logger.info(f"Created table {BIGQUERY_TABLE_REVIEWS}")
                else:
                    raise

            rows_to_insert = []
            for place_data in reviews:
                row = {
                    'place_id': place_data['place_id'],
                    'overall_rating': place_data['overall_rating'],
                    'total_ratings': place_data['total_ratings'],
                    'website': place_data['website'],
                    'reviews': place_data['reviews']
                }
                rows_to_insert.append(row)

            if rows_to_insert:
                errors = self.client.insert_rows_json(table_ref, rows_to_insert)
                if not errors:
                    logger.info(f"Successfully inserted {len(rows_to_insert)} rows")
                else:
                    logger.error(f"Errors inserting rows: {errors}")
            else:
                logger.info("No new rows to insert")

        except Exception as e:
            logger.error(f"Error saving reviews: {str(e)}", exc_info=True)
            raise 