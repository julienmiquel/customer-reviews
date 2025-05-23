import asyncio
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from bk_maps.places_client import PlacesClient
from bk_maps.bigquery_client import BigQueryClient
from bk_maps.config import LOG_DIR, LOG_FILE, REVIEW_STRATEGY
from bk_maps.logger import setup_logger

# Set up logger
logger = setup_logger(
    name="fetch_reviews",
    log_file=LOG_FILE
)

async def main():
    logger.info("Starting review fetch process")
    try:
        # Initialize clients with concurrent processing capabilities
        places_client = PlacesClient()
        bigquery_client = BigQueryClient()

        place_ids = bigquery_client.get_existing_place_ids()
        
        logger.info(f"Found {len(place_ids)} locations to process with review order by {REVIEW_STRATEGY}")
        
        # Process places and get reviews 
        reviews = places_client.get_places_details_and_reviews(
            place_ids, reviews_sort=REVIEW_STRATEGY
        )
        
        logger.info(f"Successfully fetched reviews for {len(reviews)} locations {reviews}")
        
        # Save to BigQuery 
        bigquery_client.save_reviews(
            reviews,
        )
        logger.info("Successfully saved all reviews to BigQuery")

        total_reviews = bigquery_client.get_number_of_reviews()
        logger.info(f"Total number of reviews {total_reviews} BigQuery")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Review fetch process completed")

if __name__ == "__main__":
    asyncio.run(main()) 