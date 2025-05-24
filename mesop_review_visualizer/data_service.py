from collections import Counter
from datetime import datetime
from google.cloud import bigquery
import json

# Defines data processing and fetching logic for the review visualizer.

def process_review_data(reviews_list: list) -> tuple:
    """
    Processes a list of review dictionaries to calculate aggregated metrics.
    Aggregates restaurant names using 'ui_display_name'.

    Args:
        reviews_list: A list of dictionaries, where each dictionary represents a review.

    Returns:
        A tuple containing:
            - top_pros: List of top 10 pros and their counts.
            - top_cons: List of top 10 cons and their counts.
            - average_restaurant_ratings: Dictionary of average ratings per restaurant.
            - reviews_over_time_chart_data: Dictionary formatted for time series chart.
    """
    top_pros = []
    top_cons = []
    average_restaurant_ratings = {}
    reviews_over_time_chart_data = {} # Default empty dict
    
    pros_counts = Counter()
    cons_counts = Counter()
    restaurant_ratings_agg = {} # Stores total_rating and count for each restaurant
    monthly_ts_data = {} # Stores count and total_rating for each month

    # Iterate through each review to aggregate data
    for review in reviews_list:
        # Aggregate Pros
        if review.get('review_pros'):
            pros_data = review['review_pros']
            if isinstance(pros_data, str):
                pros_counts[pros_data.strip().lower()] += 1
            elif isinstance(pros_data, list):
                for pro_item in pros_data:
                    if pro_item: # Ensure item is not None or empty
                        pros_counts[pro_item.strip().lower()] += 1
        
        # Aggregate Cons
        if review.get('review_cons'):
            cons_data = review['review_cons']
            if isinstance(cons_data, str):
                cons_counts[cons_data.strip().lower()] += 1
            elif isinstance(cons_data, list):
                for con_item in cons_data:
                    if con_item: # Ensure item is not None or empty
                        cons_counts[con_item.strip().lower()] += 1

        # Aggregate Restaurant Ratings using 'ui_display_name'
        ui_name = review.get('ui_display_name')
        review_rating = review.get('review_rating')

        if ui_name and review_rating is not None:
            if ui_name not in restaurant_ratings_agg:
                restaurant_ratings_agg[ui_name] = {'total_rating': 0.0, 'count': 0}
            try:
                restaurant_ratings_agg[ui_name]['total_rating'] += float(review_rating)
                restaurant_ratings_agg[ui_name]['count'] += 1
            except (ValueError, TypeError):
                # Log warning for ratings that cannot be converted to float
                print(f"Warning: Could not convert rating '{review_rating}' to float for restaurant '{ui_name}'.")

        # Aggregate Data for Time Series (Reviews Over Time)
        review_dt_value = review.get('review_datetime')
        if review_dt_value and review_rating is not None: # Ensure datetime and rating are present
            try:
                current_dt = review_dt_value
                if isinstance(review_dt_value, str):
                    current_dt = datetime.fromisoformat(review_dt_value) # Parse ISO format string
                elif not isinstance(review_dt_value, datetime): 
                    continue # Skip if not a datetime object or parsable string

                month_year = current_dt.strftime('%Y-%m') # Format as YYYY-MM for sorting and labeling
                if month_year not in monthly_ts_data:
                    monthly_ts_data[month_year] = {'count': 0, 'total_rating': 0.0}
                monthly_ts_data[month_year]['count'] += 1
                monthly_ts_data[month_year]['total_rating'] += float(review_rating)
            except (ValueError, TypeError) as e_dt:
                # Log warning for date or rating processing issues
                print(f"Warning: Could not process date '{review_dt_value}' or rating '{review_rating}' for time series. Error: {e_dt}")

    # Calculate Average Restaurant Ratings
    for name_key, data in restaurant_ratings_agg.items():
        if data['count'] > 0:
            average_restaurant_ratings[name_key] = round(data['total_rating'] / data['count'], 2)
        else:
            average_restaurant_ratings[name_key] = 0.0 # Default to 0.0 if no reviews
    
    # Get Top 10 Pros, filtering out empty or placeholder values
    top_pros = [ 
        (k, v) for k, v in pros_counts.most_common(10) 
        if v > 0 and k and k.strip() and k.lower() != "empty" 
    ] if pros_counts else []
    
    # Get Top 10 Cons, filtering out empty or placeholder values
    top_cons = [ 
        (k, v) for k, v in cons_counts.most_common(10) 
        if v > 0 and k and k.strip() and k.lower() != "empty"
    ] if cons_counts else []

    # Prepare Data for Reviews Over Time Chart
    if monthly_ts_data:
        sorted_months = sorted(monthly_ts_data.keys()) # Sort months chronologically
        labels = sorted_months
        review_counts_per_month = [monthly_ts_data[month]['count'] for month in sorted_months]
        average_ratings_per_month = [
            round(monthly_ts_data[month]['total_rating'] / monthly_ts_data[month]['count'], 2) 
            if monthly_ts_data[month]['count'] > 0 else 0.0
            for month in sorted_months
        ]
        reviews_over_time_chart_data = {
            'labels': labels,
            'review_counts': review_counts_per_month,
            'average_ratings': average_ratings_per_month
        }
        
    return top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data

def fetch_processed_data() -> tuple:
    """
    Fetches review data from BigQuery, processes it for UI display name disambiguation,
    and prepares data structures for map visualization.

    Returns:
        A tuple containing:
            - all_reviews_data_augmented: List of all reviews with 'ui_display_name'.
            - city_names: Sorted list of unique city names.
            - restaurants_map_data: List of dictionaries for map markers.
            - error_message: String containing an error message if an exception occurred, else None.
    """
    error_message = None
    all_reviews_data_augmented = []
    city_names = []
    restaurants_map_data = [] # For map markers: name, lat, lng, avg_rating, review_count

    # Fetch raw data from BigQuery
    try:
        client = bigquery.Client()
        # Query to fetch review data, including city for disambiguation and map display.
        # Concatenates display_name and formatted_address for a more unique initial display_name.
        query = """
            SELECT 
              IF(ENDS_WITH(formatted_address, ', France'),
                 CONCAT(display_name, " ", LEFT(formatted_address, LENGTH(formatted_address) - LENGTH(', France'))),
                 CONCAT(display_name, " ", formatted_address)
              ) AS display_name, # Original potentially ambiguous name
              city, # City extracted from address, crucial for disambiguation
              review_rating, review_pros, review_cons, review_text, review_datetime, 
              latitude, longitude
            FROM `ml-demo-384110.burger_king_reviews_currated_prod.reviews_pros_cons`
        """
        query_job = client.query(query)
        all_reviews_data_raw = [dict(row) for row in query_job.result()]
        
    except Exception as e:
        error_message = f"An error occurred while fetching data from BigQuery: {e}"
        print(error_message)
        # Return empty lists and the error message if fetching fails
        return all_reviews_data_augmented, city_names, restaurants_map_data, error_message

    # Disambiguation Logic: Create 'ui_display_name'
    # This section ensures that restaurant names are unique in the UI if they exist in multiple cities.
    name_to_cities_map = {}
    if all_reviews_data_raw:
        for review in all_reviews_data_raw:
            display_name = review.get('display_name')
            city = review.get('city')
            if display_name and city: 
                name_to_cities_map.setdefault(display_name, set()).add(city)
        
        # Identify names that appear in more than one city
        names_needing_disambiguation = {
            name for name, cities_set in name_to_cities_map.items() if len(cities_set) > 1
        }

        # Augment review data with 'ui_display_name'
        for review_raw_item in all_reviews_data_raw:
            augmented_review = review_raw_item.copy()
            original_display_name = augmented_review.get('display_name')
            city = augmented_review.get('city')
            if original_display_name in names_needing_disambiguation and city:
                # Append city to name if disambiguation is needed
                augmented_review['ui_display_name'] = f"{original_display_name} ({city})"
            else:
                # Otherwise, use the original display name
                augmented_review['ui_display_name'] = original_display_name
            all_reviews_data_augmented.append(augmented_review)
        
        # Populate unique city names from augmented data
        city_names = sorted(list(set(
            review['city'] for review in all_reviews_data_augmented if review.get('city')
        )))

    # --- Map Data Preparation ---
    # Create a unique list of restaurants with their lat/lng, average rating, and review count for map markers.
    unique_restaurants_for_map = {} 
    # Aggregate ratings and counts for each original display_name (used as map marker key)
    restaurant_aggregates_for_map = {} 

    for review in all_reviews_data_augmented: # Use augmented data to ensure all reviews are considered
        original_name = review.get('display_name') # Map data keys are original display_name
        lat = review.get('latitude')
        lng = review.get('longitude')
        rating = review.get('review_rating')

        # Store unique lat/lng for each restaurant
        if original_name and original_name not in unique_restaurants_for_map and lat is not None and lng is not None:
            try:
                unique_restaurants_for_map[original_name] = {'lat': float(lat), 'lng': float(lng)}
            except (ValueError, TypeError):
                print(f"Warning: Could not parse lat/lng for map for restaurant '{original_name}': lat={lat}, lng={lng}")
        
        # Aggregate ratings for map markers
        if original_name and rating is not None:
            if original_name not in restaurant_aggregates_for_map:
                restaurant_aggregates_for_map[original_name] = {'total_rating': 0.0, 'count': 0}
            try:
                restaurant_aggregates_for_map[original_name]['total_rating'] += float(rating)
                restaurant_aggregates_for_map[original_name]['count'] += 1
            except (ValueError, TypeError):
                 print(f"Warning: Could not parse rating '{rating}' for map aggregation for restaurant '{original_name}'.")


    # Calculate average ratings for map markers
    average_ratings_for_map = {}
    for name, data in restaurant_aggregates_for_map.items():
        if data['count'] > 0:
            average_ratings_for_map[name] = round(data['total_rating'] / data['count'], 2)
        else:
            average_ratings_for_map[name] = 0.0
            
    # Compile the final list for map markers
    for name, info in unique_restaurants_for_map.items():
        restaurants_map_data.append({
            'name': name, # Original name for map marker title
            'lat': info['lat'],
            'lng': info['lng'],
            'avg_rating': average_ratings_for_map.get(name, 0.0), # Default to 0.0 if not found
            'review_count': restaurant_aggregates_for_map.get(name, {}).get('count', 0) # Default to 0 if not found
        })
    # --- End Map Data Preparation ---
        
    return all_reviews_data_augmented, city_names, restaurants_map_data, error_message
