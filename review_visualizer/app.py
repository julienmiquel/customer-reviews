from flask import Flask, render_template, request
from google.cloud import bigquery
from collections import Counter
from datetime import datetime

app = Flask(__name__)

def process_review_data(reviews_list):
    """
    Processes a list of review data to calculate aggregated metrics.
    Args:
        reviews_list (list): A list of review dictionaries.
    Returns:
        tuple: Contains top_pros, top_cons, average_restaurant_ratings, 
               reviews_over_time_chart_data, and any processing error messages.
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
        # Process pros
        if review.get('review_pros'):
            if isinstance(review['review_pros'], str):
                pros_counts[review['review_pros'].strip().lower()] += 1
            elif isinstance(review['review_pros'], list):
                for pro in review['review_pros']:
                    if pro:
                        pros_counts[pro.strip().lower()] += 1
        
        # Process cons
        if review.get('review_cons'):
            if isinstance(review['review_cons'], str):
                cons_counts[review['review_cons'].strip().lower()] += 1
            elif isinstance(review['review_cons'], list):
                for con in review['review_cons']:
                    if con:
                        cons_counts[con.strip().lower()] += 1

        # Process restaurant ratings
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

        # Process reviews over time
        review_dt = review.get('review_datetime')
        if review_dt and review_rating is not None:
            try:
                if isinstance(review_dt, str):
                    review_dt = datetime.fromisoformat(review_dt)
                elif isinstance(review_dt, datetime): # Already a datetime object
                    pass # No action needed
                else: # Skip if not a recognizable type
                    continue

                month_year = review_dt.strftime('%Y-%m')
                if month_year not in monthly_ts_data:
                    monthly_ts_data[month_year] = {'count': 0, 'total_rating': 0.0}
                monthly_ts_data[month_year]['count'] += 1
                monthly_ts_data[month_year]['total_rating'] += float(review_rating)
            except (ValueError, TypeError) as e_dt:
                print(f"Warning: Could not process date '{review_dt}' or rating '{review_rating}' for time series. Error: {e_dt}")

    # Calculate average ratings for restaurants
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
    
    selected_restaurant_name = request.args.get('selected_restaurant_name', '')

    try:
        # This part would typically fetch from BigQuery
        # For testing, we might mock this or use a small sample if BQ is not available
        # For actual execution, BigQuery client is used:
        client = bigquery.Client()
        query = """
            SELECT display_name, review_rating, review_pros, review_cons, review_text, review_datetime
            FROM `ml-demo-384110.burger_king_reviews_currated.reviews_pros_cons`
        """
        query_job = client.query(query)
        all_reviews_data = [dict(row) for row in query_job.result()]
        
    except Exception as e:
        error_message = f"An error occurred while fetching data: {e}"
        print(error_message)
        # In case of a BQ error, all_reviews_data remains empty. Processing will yield empty results.

    if all_reviews_data:
        restaurant_names = sorted(list(set(review['display_name'] for review in all_reviews_data if review.get('display_name'))))

    # Filter reviews if a specific restaurant is selected
    if selected_restaurant_name and selected_restaurant_name != "":
        current_reviews_data = [review for review in all_reviews_data if review.get('display_name') == selected_restaurant_name]
    else:
        current_reviews_data = all_reviews_data

    # Process the current set of reviews (either all or filtered)
    top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data = process_review_data(current_reviews_data)
    
    # The individual reviews displayed on the page will be from current_reviews_data
    # This ensures that if filtered, only those reviews are shown.

    return render_template('index.html', 
                           reviews=current_reviews_data, # Send filtered or all reviews for individual display
                           top_pros=top_pros,
                           top_cons=top_cons,
                           average_restaurant_ratings=average_restaurant_ratings,
                           reviews_over_time_chart_data=reviews_over_time_chart_data,
                           restaurant_names=restaurant_names, 
                           selected_restaurant_name=selected_restaurant_name, 
                           error_message=error_message)

if __name__ == '__main__':
    # This is for local development and debugging.
    # For production, use a WSGI server like Gunicorn.
    app.run(debug=True)
