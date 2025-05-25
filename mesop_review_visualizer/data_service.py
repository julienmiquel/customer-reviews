from google.cloud import bigquery
from collections import Counter
from datetime import datetime

# Option 1: Global client (simple, often fine if client is thread-safe)
# Alternatively, the client can be cached or managed by a class.
_bq_client = None

def get_bigquery_client():
    global _bq_client
    if _bq_client is None:
        # Recommended: Set GOOGLE_APPLICATION_CREDENTIALS environment variable
        # If not set, BigQuery client tries to find credentials automatically.
        # For local development, ensure 'gcloud auth application-default login' has been run.
        _bq_client = bigquery.Client()
    return _bq_client

# BigQuery query string
BIGQUERY_QUERY = """
SELECT 
  IF(ENDS_WITH(formatted_address, ', France'),
concat(display_name," ", LEFT(formatted_address, LENGTH(formatted_address) - LENGTH(', France'))),
concat(display_name," ", formatted_address) ) as display_name,
city, 
review_rating, review_pros, review_cons, review_text, review_datetime, latitude, longitude
FROM `ml-demo-384110.burger_king_reviews_currated_prod.reviews_pros_cons`
"""

def fetch_raw_reviews():
    """
    Fetches raw review data from BigQuery.
    Returns:
        list: A list of dictionaries, where each dictionary represents a review.
              Returns an empty list if an error occurs.
    """
    client = get_bigquery_client()
    try:
        query_job = client.query(BIGQUERY_QUERY)
        reviews_list = [dict(row) for row in query_job.result()]
        return reviews_list
    except Exception as e:
        print(f"Error fetching data from BigQuery: {e}")
        # In a real app, might raise the error or log more formally
        return []

def augment_reviews_with_ui_name(raw_reviews_list):
    """
    Augments review data with a ui_display_name field for disambiguation.
    Args:
        raw_reviews_list (list): A list of raw review dictionaries from BigQuery.
                                 Each dictionary must contain 'display_name' and 'city'.
    Returns:
        list: A new list of review dictionaries, each augmented with 'ui_display_name'.
    """
    if not raw_reviews_list:
        return []

    name_to_cities = {}
    for review in raw_reviews_list:
        display_name = review.get('display_name')
        city = review.get('city') # This field is crucial from the BQ query
        if display_name and city:
            name_to_cities.setdefault(display_name, set()).add(city)
    
    names_needing_disambiguation = {
        name for name, cities in name_to_cities.items() if len(cities) > 1
    }
    
    all_reviews_data_augmented = []
    for review_data in raw_reviews_list: # Renamed to avoid confusion with outer scope 'review' if any
        augmented_review = review_data.copy() 
        original_display_name = augmented_review.get('display_name')
        city = augmented_review.get('city')
        
        if original_display_name in names_needing_disambiguation and city: 
            augmented_review['ui_display_name'] = f"{original_display_name} ({city})"
        else:
            augmented_review['ui_display_name'] = original_display_name
        all_reviews_data_augmented.append(augmented_review)
        
    return all_reviews_data_augmented

def process_review_data(reviews_list):
    """
    Processes a list of review data to calculate aggregated metrics.
    Uses 'ui_display_name' for restaurant name aggregation.
    Args:
        reviews_list (list): A list of augmented review dictionaries.
                             Each dictionary should have 'ui_display_name', 'review_rating',
                             'review_pros', 'review_cons', and 'review_datetime'.
    Returns:
        tuple: Contains top_pros (list), top_cons (list),
               average_restaurant_ratings (dict), reviews_over_time_chart_data (dict).
    """
    top_pros = []
    top_cons = []
    average_restaurant_ratings = {}
    reviews_over_time_chart_data = {}
    
    pros_counts = Counter()
    cons_counts = Counter()
    restaurant_ratings_agg = {} # Keyed by ui_display_name
    monthly_ts_data = {} # Keyed by 'YYYY-MM'

    for review in reviews_list:
        # Process pros
        review_pros = review.get('review_pros')
        if review_pros:
            if isinstance(review_pros, str):
                pros_counts[review_pros.strip().lower()] += 1
            elif isinstance(review_pros, list):
                for pro in review_pros:
                    if pro and isinstance(pro, str): # Ensure pro is a string
                        pros_counts[pro.strip().lower()] += 1
        
        # Process cons
        review_cons = review.get('review_cons')
        if review_cons:
            if isinstance(review_cons, str):
                cons_counts[review_cons.strip().lower()] += 1
            elif isinstance(review_cons, list):
                for con in review_cons:
                    if con and isinstance(con, str): # Ensure con is a string
                        cons_counts[con.strip().lower()] += 1

        # Aggregate ratings by ui_display_name
        ui_name = review.get('ui_display_name')
        review_rating = review.get('review_rating')

        if ui_name and review_rating is not None:
            if ui_name not in restaurant_ratings_agg:
                restaurant_ratings_agg[ui_name] = {'total_rating': 0.0, 'count': 0}
            try:
                restaurant_ratings_agg[ui_name]['total_rating'] += float(review_rating)
                restaurant_ratings_agg[ui_name]['count'] += 1
            except (ValueError, TypeError):
                print(f"Warning: Could not convert rating '{review_rating}' to float for {ui_name}.")

        # Process data for time series
        review_dt = review.get('review_datetime') 
        if review_dt and review_rating is not None:
            current_dt = None
            if isinstance(review_dt, datetime):
                current_dt = review_dt
            elif isinstance(review_dt, str):
                try:
                    # Attempt to parse common ISO-like formats
                    if "T" in review_dt and ("Z" in review_dt or "+" in review_dt.split("T")[-1]): # Handles 'Z' and timezone offsets
                        current_dt = datetime.fromisoformat(review_dt.replace('Z', '+00:00'))
                    elif " " in review_dt and "." in review_dt: # e.g. '2023-04-12 10:30:00.123'
                         current_dt = datetime.fromisoformat(review_dt)
                    elif " " in review_dt: # e.g. '2023-04-12 10:30:00'
                         current_dt = datetime.fromisoformat(review_dt)
                    else: # Assuming YYYY-MM-DD if no time part and not a full iso string
                         current_dt = datetime.strptime(review_dt, '%Y-%m-%d')
                except ValueError as e_parse:
                    print(f"Warning: Could not parse date string '{review_dt}'. Error: {e_parse}")
                    continue # Skip this review for time series if date parsing fails
            else:
                # If review_dt is not a datetime object or a string, skip for time series
                print(f"Warning: review_datetime has unexpected type {type(review_dt)}. Skipping for time series.")
                continue
            
            if current_dt: # Ensure current_dt was successfully parsed/assigned
                month_year = current_dt.strftime('%Y-%m')
                if month_year not in monthly_ts_data:
                    monthly_ts_data[month_year] = {'count': 0, 'total_rating': 0.0, 'ratings_sum': 0.0} # Ensure float for sum
                monthly_ts_data[month_year]['count'] += 1
                try:
                    monthly_ts_data[month_year]['ratings_sum'] += float(review_rating)
                except (ValueError, TypeError):
                     print(f"Warning: Could not convert rating '{review_rating}' to float for time series in {month_year}.")


    # Calculate average ratings for each restaurant
    for name_key, data in restaurant_ratings_agg.items():
        if data['count'] > 0:
            average_restaurant_ratings[name_key] = round(data['total_rating'] / data['count'], 2)
        else:
            average_restaurant_ratings[name_key] = 0.0 # Ensure float
    
    # Get top 10 pros and cons, filtering out empty/None strings
    top_pros = [ (k, v) for k, v in pros_counts.most_common(10) if v > 0 and k and k.strip() and k.lower() != "empty" ]
    top_cons = [ (k, v) for k, v in cons_counts.most_common(10) if v > 0 and k and k.strip() and k.lower() != "empty" ]

    # Prepare time series chart data
    if monthly_ts_data:
        sorted_months = sorted(monthly_ts_data.keys())
        labels = sorted_months
        review_counts_per_month = [monthly_ts_data[month]['count'] for month in sorted_months]
        average_ratings_per_month = [
            round(monthly_ts_data[month]['ratings_sum'] / monthly_ts_data[month]['count'], 2) if monthly_ts_data[month]['count'] > 0 else 0.0 # Ensure float
            for month in sorted_months
        ]
        reviews_over_time_chart_data = {
            'labels': labels,
            'review_counts': review_counts_per_month,
            'average_ratings': average_ratings_per_month
        }
    else: # Ensure structure is present even if empty
        reviews_over_time_chart_data = {'labels': [], 'review_counts': [], 'average_ratings': []}
        
    return top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data

def get_processed_review_data():
    """
    Fetches raw reviews, augments them with ui_display_name.
    This is the main service function to be called by the UI initially.
    In future steps, this function might also call process_review_data directly
    or that might be handled by another layer depending on UI needs.
    Returns:
        list: A list of augmented review dictionaries.
    """
    raw_reviews = fetch_raw_reviews()
    if not raw_reviews:
        print("No raw reviews fetched. Returning empty list.")
        return []
    
    augmented_reviews = augment_reviews_with_ui_name(raw_reviews)
    if not augmented_reviews:
        print("Augmentation resulted in empty list. Returning empty list.")
        return []
        
    # For now, as per subtask, just return the augmented reviews.
    # The full processing (calling process_review_data) will be integrated later.
    return augmented_reviews
