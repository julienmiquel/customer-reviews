from flask import Flask, render_template, request
from google.cloud import bigquery
from collections import Counter
from datetime import datetime
import json # Import json for tojson filter if needed, though Flask's tojson is usually available

app = Flask(__name__)

def process_review_data(reviews_list):
    """
    Processes a list of review data to calculate aggregated metrics.
    Args:
        reviews_list (list): A list of review dictionaries.
    Returns:
        tuple: Contains top_pros, top_cons, average_restaurant_ratings, 
               reviews_over_time_chart_data.
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

        display_name = review.get('display_name')
        review_rating = review.get('review_rating')

        if display_name and review_rating is not None:
            if display_name not in restaurant_ratings_agg:
                restaurant_ratings_agg[display_name] = {'total_rating': 0, 'count': 0}
            try:
                restaurant_ratings_agg[display_name]['total_rating'] += float(review_rating)
                restaurant_ratings_agg[display_name]['count'] += 1
            except (ValueError, TypeError):
                print(f"Warning: Could not convert rating '{review_rating}' to float for {display_name}.")

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

    for name, data in restaurant_ratings_agg.items():
        if data['count'] > 0:
            average_restaurant_ratings[name] = round(data['total_rating'] / data['count'], 2)
        else:
            average_restaurant_ratings[name] = 0
    
    top_pros = pros_counts.most_common(10)
    top_cons = cons_counts.most_common(10)

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
    restaurant_names = []
    all_reviews_data = []
    restaurants_map_data = []
    unique_restaurants_info = {}
    
    selected_restaurant_name = request.args.get('selected_restaurant_name', '')

    try:
        bq_client_config = app.config.get('BIGQUERY_CLIENT')
        if bq_client_config is not None: # Use mock/None in tests
             client = bq_client_config
        else: # Actual client for production/development
             client = bigquery.Client()
        
        # 1.a. Ensure query selects latitude and longitude
        query = """
            SELECT display_name, review_rating, review_pros, review_cons, review_text, review_datetime, latitude, longitude
            FROM `ml-demo-384110.burger_king_reviews_currated.reviews_pros_cons`
        """
        query_job = client.query(query)
        all_reviews_data = [dict(row) for row in query_job.result()]
        
    except Exception as e:
        error_message = f"An error occurred while fetching data: {e}"
        print(error_message)

    # 1.b.i. Create unique_restaurants_info with first valid lat/lng
    if all_reviews_data:
        for review in all_reviews_data:
            name = review.get('display_name')
            lat = review.get('latitude')
            lng = review.get('longitude')
            if name and name not in unique_restaurants_info and lat is not None and lng is not None:
                try:
                    # Ensure lat/lng are floats
                    unique_restaurants_info[name] = {'lat': float(lat), 'lng': float(lng)}
                except (ValueError, TypeError):
                    print(f"Warning: Could not parse lat/lng for {name}: lat={lat}, lng={lng}")
        
        restaurant_names = sorted(list(unique_restaurants_info.keys()))


    # 1.b.iii. Calculate all_restaurant_aggregates for map data (always from all_reviews_data)
    all_restaurant_aggregates = {}
    for review in all_reviews_data:
        name = review.get('display_name')
        rating = review.get('review_rating')
        if name and rating is not None:
            if name not in all_restaurant_aggregates:
                all_restaurant_aggregates[name] = {'total_rating': 0, 'count': 0}
            try:
                all_restaurant_aggregates[name]['total_rating'] += float(rating)
                all_restaurant_aggregates[name]['count'] += 1
            except (ValueError, TypeError):
                pass # Already warned during main processing if it happens

    all_average_ratings = {}
    for name, data in all_restaurant_aggregates.items():
        if data['count'] > 0:
            all_average_ratings[name] = round(data['total_rating'] / data['count'], 2)
        else:
            all_average_ratings[name] = 0
            
    # Construct restaurants_map_data
    for name, info in unique_restaurants_info.items():
        restaurants_map_data.append({
            'name': name,
            'lat': info['lat'],
            'lng': info['lng'],
            'avg_rating': all_average_ratings.get(name, 0),
            'review_count': all_restaurant_aggregates.get(name, {}).get('count', 0)
        })

    # Filter reviews for dashboard display if a specific restaurant is selected
    if selected_restaurant_name and selected_restaurant_name != "":
        current_reviews_data = [review for review in all_reviews_data if review.get('display_name') == selected_restaurant_name]
    else:
        current_reviews_data = all_reviews_data

    # Process data for charts/tables based on current (potentially filtered) selection
    top_pros, top_cons, average_restaurant_ratings_display, reviews_over_time_chart_data = process_review_data(current_reviews_data)
    # Note: average_restaurant_ratings_display is for the charts, which respect the filter.
    # restaurants_map_data uses all_average_ratings which is for all restaurants.
    
    return render_template('index.html', 
                           reviews=current_reviews_data, 
                           top_pros=top_pros,
                           top_cons=top_cons,
                           average_restaurant_ratings=average_restaurant_ratings_display, # For the ratings chart
                           reviews_over_time_chart_data=reviews_over_time_chart_data,
                           restaurant_names=restaurant_names, 
                           selected_restaurant_name=selected_restaurant_name, 
                           restaurants_map_data=restaurants_map_data, # 1.b.iv. Pass to template
                           error_message=error_message)

if __name__ == '__main__':
    app.run(debug=True)
```
