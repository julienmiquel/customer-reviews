from flask import Flask, render_template, request
from google.cloud import bigquery
from collections import Counter
from datetime import datetime
import json # Import json for tojson filter if needed, though Flask's tojson is usually available

app = Flask(__name__)

# 2.a. Update process_review_data to use ui_display_name
def process_review_data(reviews_list):
    """
    Processes a list of review data to calculate aggregated metrics.
    Uses 'ui_display_name' for restaurant name aggregation.
    """
    top_pros = []
    top_cons = []
    average_restaurant_ratings = {}
    reviews_over_time_chart_data = {}
    
    pros_counts = Counter()
    cons_counts = Counter()
    restaurant_ratings_agg = {}
    monthly_ts_data = {}

    for review in reviews_list:
        if review.get('review_pros'):
            if isinstance(review['review_pros'], str):
                pros_counts[review['review_pros'].strip().lower()] += 1
            elif isinstance(review['review_pros'], list):
                for pro in review['review_pros']:
                    if pro:
                        pros_counts[pro.strip().lower()] += 1
        
        if review.get('review_cons'):
            if isinstance(review['review_cons'], str):
                cons_counts[review['review_cons'].strip().lower()] += 1
            elif isinstance(review['review_cons'], list):
                for con in review['review_cons']:
                    if con:
                        cons_counts[con.strip().lower()] += 1

        # Use 'ui_display_name' for aggregation key
        ui_name = review.get('ui_display_name') # Changed from display_name
        review_rating = review.get('review_rating')

        if ui_name and review_rating is not None:
            if ui_name not in restaurant_ratings_agg:
                restaurant_ratings_agg[ui_name] = {'total_rating': 0, 'count': 0}
            try:
                restaurant_ratings_agg[ui_name]['total_rating'] += float(review_rating)
                restaurant_ratings_agg[ui_name]['count'] += 1
            except (ValueError, TypeError):
                print(f"Warning: Could not convert rating '{review_rating}' to float for {ui_name}.")

        review_dt = review.get('review_datetime')
        if review_dt and review_rating is not None:
            try:
                current_dt = review_dt
                if isinstance(review_dt, str):
                    current_dt = datetime.fromisoformat(review_dt)
                elif not isinstance(review_dt, datetime): 
                    continue 

                month_year = current_dt.strftime('%Y-%m')
                if month_year not in monthly_ts_data:
                    monthly_ts_data[month_year] = {'count': 0, 'total_rating': 0.0}
                monthly_ts_data[month_year]['count'] += 1
                monthly_ts_data[month_year]['total_rating'] += float(review_rating)
            except (ValueError, TypeError) as e_dt:
                print(f"Warning: Could not process date '{review_dt}' or rating '{review_rating}' for time series. Error: {e_dt}")

    for name_key, data in restaurant_ratings_agg.items(): # name_key is now ui_display_name
        if data['count'] > 0:
            average_restaurant_ratings[name_key] = round(data['total_rating'] / data['count'], 2)
        else:
            average_restaurant_ratings[name_key] = 0
    
    top_pros = [ (k, v) for k, v in pros_counts.most_common(10) if v > 0 and k is not None and len(k) > 0 and k != "" and k != "empty" ] if pros_counts else []
    top_cons = [ (k, v) for k, v in cons_counts.most_common(10) if v > 0 and k is not None and len(k) > 0 and k != "" and k != "empty" ] if cons_counts else []

    if monthly_ts_data:
        sorted_months = sorted(monthly_ts_data.keys())
        labels = sorted_months
        review_counts_per_month = [monthly_ts_data[month]['count'] for month in sorted_months]
        average_ratings_per_month = [
            round(monthly_ts_data[month]['total_rating'] / monthly_ts_data[month]['count'], 2) if monthly_ts_data[month]['count'] > 0 else 0
            for month in sorted_months
        ]
        reviews_over_time_chart_data = {
            'labels': labels,
            'review_counts': review_counts_per_month,
            'average_ratings': average_ratings_per_month
        }
        
    return top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data

@app.route('/')
def index():
    error_message = None
    city_names = [] 
    all_reviews_data_augmented = [] # Will store reviews with ui_display_name
    restaurants_map_data = []
    
    selected_restaurant_name = request.args.get('selected_restaurant_name', '') # This will now be a ui_display_name
    selected_city = request.args.get('selected_city', '') 

    try:
        bq_client_config = app.config.get('BIGQUERY_CLIENT')
        if bq_client_config is not None: 
             client = bq_client_config
        else: 
             client = bigquery.Client()
        
        query = """
            SELECT display_name, review_rating, review_pros, review_cons, review_text, review_datetime, latitude, longitude
            FROM `ml-demo-384110.burger_king_reviews_currated_prod.reviews_pros_cons`
        """
        query_job = client.query(query)
        all_reviews_data_raw = [dict(row) for row in query_job.result()] # Raw data from BQ
        
    except Exception as e:
        error_message = f"An error occurred while fetching data: {e}"
        print(error_message)
        all_reviews_data_raw = [] # Ensure it's an empty list on error

    # 1.a. Disambiguation logic
    name_to_cities = {}
    if all_reviews_data_raw:
        for review in all_reviews_data_raw:
            display_name = review.get('display_name')
            city = review.get('city')
            if display_name and city:
                name_to_cities.setdefault(display_name, set()).add(city)
        
        names_needing_disambiguation = {name for name, cities in name_to_cities.items() if len(cities) > 1}

        for review in all_reviews_data_raw:
            augmented_review = review.copy() # Create a copy to avoid modifying original dict from BQ results directly
            original_display_name = augmented_review.get('display_name')
            city = augmented_review.get('city')
            if original_display_name in names_needing_disambiguation and city: # Ensure city is present for disambiguation
                augmented_review['ui_display_name'] = f"{original_display_name} ({city})"
            else:
                augmented_review['ui_display_name'] = original_display_name
            all_reviews_data_augmented.append(augmented_review)
        
        city_names = sorted(list(set(review['city'] for review in all_reviews_data_augmented if review.get('city'))))

    # --- Map Data Preparation (uses all_reviews_data_augmented for consistency, though map keys are original names) ---
    unique_restaurants_info_map = {} # For map lat/lng, keys are original display_name
    for review in all_reviews_data_augmented: # Use augmented data to ensure all reviews are considered
        original_name = review.get('display_name') # Map data should still key by original name for lat/lng
        lat = review.get('latitude')
        lng = review.get('longitude')
        if original_name and original_name not in unique_restaurants_info_map and lat is not None and lng is not None:
            try:
                unique_restaurants_info_map[original_name] = {'lat': float(lat), 'lng': float(lng)}
            except (ValueError, TypeError):
                print(f"Warning: Could not parse lat/lng for {original_name}: lat={lat}, lng={lng}")

    all_restaurant_aggregates_map = {} 
    for review in all_reviews_data_augmented: 
        original_name = review.get('display_name') # Aggregates for map use original name
        rating = review.get('review_rating')
        if original_name and rating is not None:
            if original_name not in all_restaurant_aggregates_map:
                all_restaurant_aggregates_map[original_name] = {'total_rating': 0, 'count': 0}
            try:
                all_restaurant_aggregates_map[original_name]['total_rating'] += float(rating)
                all_restaurant_aggregates_map[original_name]['count'] += 1
            except (ValueError, TypeError):
                pass 

    all_average_ratings_map = {}
    for name, data in all_restaurant_aggregates_map.items():
        if data['count'] > 0:
            all_average_ratings_map[name] = round(data['total_rating'] / data['count'], 2)
        else:
            all_average_ratings_map[name] = 0
            
    for name, info in unique_restaurants_info_map.items():
        restaurants_map_data.append({
            'name': name, # Original name for map marker title
            'lat': info['lat'],
            'lng': info['lng'],
            'avg_rating': all_average_ratings_map.get(name, 0),
            'review_count': all_restaurant_aggregates_map.get(name, {}).get('count', 0)
        })
    # --- End Map Data Preparation ---

    # Filtering Logic using ui_display_name
    temp_reviews_data_for_filtering = all_reviews_data_augmented
    
    if selected_city:
        temp_reviews_data_for_filtering = [r for r in temp_reviews_data_for_filtering if r.get('city') == selected_city]

    # 1.b. Update restaurant_names for dropdown using ui_display_name
    if selected_city:
        current_restaurant_names_for_dropdown = sorted(list(set(r['ui_display_name'] for r in temp_reviews_data_for_filtering if r.get('ui_display_name'))))
    else:
        current_restaurant_names_for_dropdown = sorted(list(set(r['ui_display_name'] for r in all_reviews_data_augmented if r.get('ui_display_name'))))

    # 1.c. Update filtering by restaurant name using ui_display_name
    if selected_restaurant_name: # selected_restaurant_name is now a ui_display_name
        current_reviews_data_for_display = [r for r in temp_reviews_data_for_filtering if r.get('ui_display_name') == selected_restaurant_name]
    else:
        current_reviews_data_for_display = temp_reviews_data_for_filtering

    # 1.d. Ensure process_review_data uses ui_display_name (already handled in process_review_data)
    top_pros, top_cons, average_restaurant_ratings_display, reviews_over_time_chart_data = process_review_data(current_reviews_data_for_display)
    
    total_displayed_reviews = len(current_reviews_data_for_display)
    total_distinct_cities = len(city_names) 

    return render_template('index.html', 
                           reviews=current_reviews_data_for_display, 
                           top_pros=top_pros,
                           top_cons=top_cons,
                           average_restaurant_ratings=average_restaurant_ratings_display, 
                           reviews_over_time_chart_data=reviews_over_time_chart_data,
                           restaurant_names=current_restaurant_names_for_dropdown, 
                           city_names=city_names, 
                           selected_restaurant_name=selected_restaurant_name, 
                           selected_city=selected_city, 
                           restaurants_map_data=restaurants_map_data, 
                           total_displayed_reviews=total_displayed_reviews, 
                           total_distinct_cities=total_distinct_cities,   
                           error_message=error_message)

if __name__ == '__main__':
    app.run(debug=True)

