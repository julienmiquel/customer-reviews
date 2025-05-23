import asyncio
import time
import requests
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from google.maps import places_v1
from google.type import latlng_pb2

from .config import API_KEY
from .logger import setup_logger

PLACE_DETAILS_URL = 'https://maps.googleapis.com/maps/api/place/details/json'
SLEEP_DURATION = 0.05
logger = setup_logger(__name__)

class PlacesClient:
    def __init__(self):
        self.client = places_v1.PlacesAsyncClient(
            client_options={"api_key": API_KEY}
        )
        logger.info("PlacesClient initialized")

    async def text_search(self, search_query: str):
        """Search for places using text query."""
        logger.info(f"Performing text search for: {search_query}")
        try:
            request = places_v1.SearchTextRequest(
                text_query=search_query,
                included_type="hamburger_restaurant",
            )
            fieldMask = "*"
            fieldMask = "places.formattedAddress,places.displayName,places.id,places.location"

            response = await self.client.search_text(
                request=request,
                metadata=[("x-goog-fieldmask", fieldMask)]
            )
            logger.info(f"Found {len(response.places)} places")
            return response
        except Exception as e:
            logger.error(f"Error in text search: {str(e)}", exc_info=True)
            raise


    def get_place_details_and_reviews(self, place_id, language='fr', reviews_sort='newest'):
        """
        Fetches place details, including reviews, using Google Places API Place Details.
        Args:
            api_key (str): Your Google Maps API Key.
            place_id (str): The Place ID of the location.
            language (str): The language code for results.
            reviews_sort (str): How to sort reviews ('newest' or 'most_relevant').
        Returns:
            dict: A dictionary containing place details and reviews if successful, None otherwise.
        """
        # Fields to retrieve: name, formatted_address, rating, reviews (author_name, rating, text, relative_time_description)
        # Note: The API typically returns up to 5 reviews.
        fields = 'name,formatted_address,rating,reviews,website,user_ratings_total'
        params = {
            'place_id': place_id,
            'fields': fields,
            'key': API_KEY,
            'language': language,
            'reviews_sort': reviews_sort # 'newest' or 'most_relevant'
        }
        try:
            response = requests.get(PLACE_DETAILS_URL, params=params)
            response.raise_for_status()
            details = response.json()

            if details['status'] == 'OK':
                return details.get('result', {})
            else:
                print(f"Error in Place Details API for place_id {place_id}: {details['status']}")
                if 'error_message' in details:
                    print(f"Error message: {details['error_message']}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for place_id {place_id}: {e}")
            return None
        except json.JSONDecodeError:
            print(f"Failed to decode JSON response from Place Details API for place_id {place_id}.")
            return None

    def get_places_details_and_reviews(
        self, 
        places_id: List[str], 
        language: str = 'fr', 
        reviews_sort: str = 'newest'
    ) -> List[Dict[str, Any]]:
        """Get details and reviews for multiple places."""
        logger.info(f"Processing {len(places_id)} places")
        all_burger_king_reviews = []
        
        for i, place_id in enumerate(places_id, 1):
            logger.info(f"Processing location {i}/{len(places_id)} (Place ID: {place_id})")
            try:
                details = self.get_place_details_and_reviews(
                    place_id, 
                    language=language, 
                    reviews_sort=reviews_sort
                )
                
                if details:
                    restaurant_name = details.get('name', 'N/A')
                    logger.info(f"Successfully fetched details for: {restaurant_name}")
                    
                    reviews = details.get('reviews', [])
                    if reviews:
                        logger.info(f"Found {len(reviews)} reviews for {restaurant_name}")
                        all_burger_king_reviews.append({
                            'place_id': place_id,
                            'overall_rating': details.get('rating', 'N/A'),
                            'total_ratings': details.get('user_ratings_total', 'N/A'),
                            'website': details.get('website', 'N/A'),
                            'reviews': reviews
                        })
                    else:
                        logger.warning(f"No reviews found for {restaurant_name}")
                
                if i < len(places_id):
                    logger.debug(f"Sleeping for {SLEEP_DURATION} seconds to respect API rate limits")
                    time.sleep(SLEEP_DURATION)
                    
            except Exception as e:
                logger.error(f"Error processing place {place_id}: {str(e)}", exc_info=True)
                continue
                
        return all_burger_king_reviews 