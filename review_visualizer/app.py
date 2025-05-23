from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
from collections import Counter
from datetime import datetime
import json 

app = Flask(__name__)
debug_mode = False

def augment_reviews_with_ui_name_globally(reviews_raw_list):
    name_to_cities = {}
    for review in reviews_raw_list:
        display_name = review.get('display_name')
        city = review.get('city')
        if display_name and city:
            name_to_cities.setdefault(display_name, set()).add(city)
    
    names_needing_disambiguation = {name for name, cities in name_to_cities.items() if len(cities) > 1}
    
    augmented_reviews = []
    for review in reviews_raw_list:
        r_copy = review.copy()
        original_display_name = r_copy.get('display_name')
        city = r_copy.get('city')
        if original_display_name in names_needing_disambiguation and city:
            r_copy['ui_display_name'] = f"{original_display_name} ({city})"
        else:
            r_copy['ui_display_name'] = original_display_name
        augmented_reviews.append(r_copy)
    return augmented_reviews

def process_review_data(reviews_list):
    top_pros = []
    top_cons = []
    average_restaurant_ratings = {}
    reviews_over_time_chart_data = {}
    pros_counts = Counter()
    cons_counts = Counter()
    restaurant_ratings_agg = {}
    monthly_ts_data = {}

    print(f"Processing {len(reviews_list)} reviews" )


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


        ui_name = review.get('ui_display_name') 
        review_rating = review.get('review_rating')

        if ui_name and review_rating is not None:
            if ui_name not in restaurant_ratings_agg:
                restaurant_ratings_agg[ui_name] = {'total_rating': 0, 'count': 0}
            try:
                restaurant_ratings_agg[ui_name]['total_rating'] += float(review_rating)
                restaurant_ratings_agg[ui_name]['count'] += 1
            except (ValueError, TypeError): pass

        review_dt = review.get('review_datetime')
        if review_dt and review_rating is not None:
            try:
                current_dt = review_dt
                if isinstance(review_dt, str): current_dt = datetime.fromisoformat(review_dt)
                elif not isinstance(review_dt, datetime): continue 
                month_year = current_dt.strftime('%Y-%m')
                if month_year not in monthly_ts_data:
                    monthly_ts_data[month_year] = {'count': 0, 'total_rating': 0.0}
                monthly_ts_data[month_year]['count'] += 1
                monthly_ts_data[month_year]['total_rating'] += float(review_rating)
            except (ValueError, TypeError): pass

    for name_key, data in restaurant_ratings_agg.items(): 
        average_restaurant_ratings[name_key] = round(data['total_rating'] / data['count'], 2) if data['count'] > 0 else 0
    
    top_pros =  [ (k, v) for k, v in pros_counts.most_common(10) if v > 0 and k is not None and len(k) > 0 and k != ""] if pros_counts else []
    top_cons = [ (k, v) for k, v in cons_counts.most_common(10) if v > 0 and k is not None and len(k) > 0 and k != ""] if cons_counts else []

    if monthly_ts_data:
        sorted_months = sorted(monthly_ts_data.keys())
        reviews_over_time_chart_data = {
            'labels': sorted_months,
            'review_counts': [monthly_ts_data[month]['count'] for month in sorted_months],
            'average_ratings': [round(monthly_ts_data[month]['total_rating'] / monthly_ts_data[month]['count'], 2) if monthly_ts_data[month]['count'] > 0 else 0 for month in sorted_months]
        }
    return top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data

def get_and_augment_all_reviews():
    all_reviews_data_raw = []
    try:
        bq_client_config = app.config.get('BIGQUERY_CLIENT')
        client = bq_client_config if bq_client_config is not None else bigquery.Client()
        query = """
        SELECT 
                        IF(ENDS_WITH(formatted_address, ', France'),
    concat(display_name," ", LEFT(formatted_address, LENGTH(formatted_address) - LENGTH(', France'))),
    concat(display_name," ", formatted_address) ) as display_name,

            review_rating,
            review_pros,
            review_cons,
            review_text,
            review_datetime,
            latitude,
            longitude,
            formatted_address as city,
            -- Add these fields to help diagnose
            CASE 
                WHEN latitude IS NULL THEN 'NULL latitude'
                WHEN longitude IS NULL THEN 'NULL longitude'
                WHEN NOT (latitude BETWEEN -90 AND 90) THEN 'Invalid latitude range'
                WHEN NOT (longitude BETWEEN -180 AND 180) THEN 'Invalid longitude range'
                ELSE 'Valid coordinates'
            END as coordinate_status
        FROM `ml-demo-384110.burger_king_reviews_currated_prod.reviews_pros_cons`
        """
        query_job = client.query(query)
        all_reviews_data_raw = [dict(row) for row in query_job.result()]
        print(f"Loaded {len(all_reviews_data_raw)} reviews from BigQuery")
        
        # Print coordinate status summary
        coord_status_counts = {}
        for row in all_reviews_data_raw:
            status = row.get('coordinate_status', 'Unknown')
            coord_status_counts[status] = coord_status_counts.get(status, 0) + 1
        if debug_mode:
            print("\nCoordinate Status Summary:")
            for status, count in coord_status_counts.items():
                print(f"{status}: {count}")
                
                
            # Print sample of coordinate values
            print("\nSample of coordinate values (first 5 records):")
            for i, row in enumerate(all_reviews_data_raw[:5]):
                print(f"Record {i+1}:")
                print(f"  Latitude: {row.get('latitude')} (type: {type(row.get('latitude'))})")
                print(f"  Longitude: {row.get('longitude')} (type: {type(row.get('longitude'))})")
            
    except Exception as e:
        print(f"Error fetching data from BigQuery: {e}")
        return [] 
    return augment_reviews_with_ui_name_globally(all_reviews_data_raw)

@app.route('/')
def index():
    error_message = None
    city_names_for_initial_filter = [] 
    restaurants_map_data = [] # For map markers, always all unique restaurants with coords
    
    selected_restaurant_name_ui = request.args.get('selected_restaurant_name', '') 
    selected_city_filter = request.args.get('selected_city', '') 

    all_reviews_data_augmented = get_and_augment_all_reviews()
    if not all_reviews_data_augmented: error_message = "Failed to load review data."

    # Prepare map data (always all unique restaurants with valid lat/lng)
    unique_restaurants_info_map = {}
    if all_reviews_data_augmented:
        # First, collect all unique restaurant names
        unique_restaurant_names = set()
        restaurants_with_coords = set()
        coord_errors = []
        
        if debug_mode:
        # Print sample of restaurant names
            print("\nSample of restaurant names (first 10 records):")
            for i, review in enumerate(all_reviews_data_augmented[:10]):
                print(f"Record {i+1}:")
                print(f"  Display name: {review.get('display_name')}")
                print(f"  UI display name: {review.get('ui_display_name')}")
                print(f"  City: {review.get('city')}")
        
        for review in all_reviews_data_augmented: 
            original_name = review.get('display_name') 
            if original_name:
                unique_restaurant_names.add(original_name)
                lat, lng = review.get('latitude'), review.get('longitude')
                if lat is not None and lng is not None:
                    try: 
                        lat_float = float(lat)
                        lng_float = float(lng)
                        if -90 <= lat_float <= 90 and -180 <= lng_float <= 180:
                            unique_restaurants_info_map[original_name] = {'lat': lat_float, 'lng': lng_float}
                            restaurants_with_coords.add(original_name)
                        else:
                            coord_errors.append(f"Invalid range for {original_name}: lat={lat_float}, lng={lng_float}")
                    except (ValueError, TypeError) as e:
                        coord_errors.append(f"Conversion error for {original_name}: lat={lat}, lng={lng}, error={str(e)}")
        if debug_mode:
            print(f"\nTotal unique restaurants: {len(unique_restaurant_names)}")
            print(f"Restaurants with valid coordinates: {len(restaurants_with_coords)}")
            print(f"Restaurants missing coordinates: {len(unique_restaurant_names) - len(restaurants_with_coords)}")
            print("\nUnique restaurant names:")
            for name in sorted(unique_restaurant_names):
                print(f"  {name}")
            if coord_errors:
                print("\nCoordinate errors (first 5):")
                for error in coord_errors[:5]:
                    print(f"  {error}")
        
        city_names_for_initial_filter = sorted(list(set(review['city'] for review in all_reviews_data_augmented if review.get('city'))))

    all_restaurant_aggregates_map = {} 
    for review in all_reviews_data_augmented: 
        original_name, rating = review.get('display_name'), review.get('review_rating')
        if original_name and rating is not None:
            if original_name not in all_restaurant_aggregates_map: all_restaurant_aggregates_map[original_name] = {'total_rating': 0, 'count': 0}
            try:
                all_restaurant_aggregates_map[original_name]['total_rating'] += float(rating)
                all_restaurant_aggregates_map[original_name]['count'] += 1
            except (ValueError, TypeError): pass 
    all_average_ratings_map = {name: round(data['total_rating'] / data['count'], 2) if data['count'] > 0 else 0 for name, data in all_restaurant_aggregates_map.items()}
    for name, info in unique_restaurants_info_map.items():
        restaurants_map_data.append({'name': name, 'lat': info['lat'], 'lng': info['lng'], 'avg_rating': all_average_ratings_map.get(name, 0), 'review_count': all_restaurant_aggregates_map.get(name, {}).get('count', 0)})

    # Initial filtering for dashboard display based on query params
    current_reviews_for_dashboard = all_reviews_data_augmented
    # if selected_city_filter:
    #     current_reviews_for_dashboard = [r for r in current_reviews_for_dashboard if r.get('city') == selected_city_filter]
    
    restaurant_names_for_initial_dropdown = sorted(list(set(r['ui_display_name'] for r in current_reviews_for_dashboard if r.get('ui_display_name')))) if selected_city_filter else sorted(list(set(r['ui_display_name'] for r in all_reviews_data_augmented if r.get('ui_display_name'))))

    # if selected_restaurant_name_ui: 
    #     current_reviews_for_dashboard = [r for r in current_reviews_for_dashboard if r.get('ui_display_name') == selected_restaurant_name_ui]

    top_pros, top_cons, avg_ratings_display, time_chart_data = process_review_data(current_reviews_for_dashboard)
    
    return render_template('index.html', 
                           reviews=current_reviews_for_dashboard, # Sample of reviews for display
                           top_pros=top_pros, top_cons=top_cons,
                           average_restaurant_ratings=avg_ratings_display, 
                           reviews_over_time_chart_data=time_chart_data,
                           restaurant_names=restaurant_names_for_initial_dropdown, 
                           city_names=city_names_for_initial_filter, 
                           selected_restaurant_name=selected_restaurant_name_ui, 
                           selected_city=selected_city_filter, 
                           restaurants_map_data=restaurants_map_data, 
                           total_displayed_reviews=len(current_reviews_for_dashboard), 
                           total_distinct_cities=len(city_names_for_initial_filter),   
                           error_message=error_message)

# 2.a. Modify /update_dashboard_by_map_bounds endpoint
@app.route('/update_dashboard_by_map_bounds')
def update_dashboard_by_map_bounds():
    try: # 2.a.i. Accept optional filters
        ne_lat = float(request.args.get('ne_lat'))
        ne_lng = float(request.args.get('ne_lng'))
        sw_lat = float(request.args.get('sw_lat'))
        sw_lng = float(request.args.get('sw_lng'))
        selected_city_from_js = request.args.get('selected_city', '')
        selected_restaurant_from_js = request.args.get('selected_restaurant_name', '')
    except (TypeError, ValueError):
        return jsonify({'error': 'Missing or invalid geographic bounds'}), 400

    all_reviews_data_augmented = get_and_augment_all_reviews()
    if not all_reviews_data_augmented:
         return jsonify({'error': 'Failed to load review data for map bounds processing'}), 500

    # 2.a.ii. Filtering Order:
    # 1. Geographic filtering
    geo_filtered_reviews = []
    for review in all_reviews_data_augmented:
        lat, lng = review.get('latitude'), review.get('longitude')
        if lat is not None and lng is not None:
            try:
                review_lat, review_lng = float(lat), float(lng)
                if sw_lat <= review_lat <= ne_lat and sw_lng <= review_lng <= ne_lng:
                    geo_filtered_reviews.append(review)
            except (ValueError, TypeError): continue
    
    # Data for dropdowns should be based on geo_filtered_reviews *before* further filtering by dropdown values themselves.
    # 2.a.iv. (Revised logic for dropdowns)
    city_names_for_dropdown = sorted(list(set(r['city'] for r in geo_filtered_reviews if r.get('city'))))
    
    # If a city is selected in the dropdowns, restaurant dropdown should only show restaurants from that city within the map view.
    # Otherwise, show all restaurants in the map view.
    reviews_for_restaurant_dropdown = geo_filtered_reviews
    if selected_city_from_js: # Filter by the city from dropdown for restaurant list
        reviews_for_restaurant_dropdown = [r for r in geo_filtered_reviews if r.get('city') == selected_city_from_js]
    restaurant_names_for_dropdown = sorted(list(set(r['ui_display_name'] for r in reviews_for_restaurant_dropdown if r.get('ui_display_name'))))

    # Now, apply dropdown filters to the geo_filtered_reviews for chart/stat processing
    final_filtered_reviews = geo_filtered_reviews
    # 2. If selected_city is provided, further filter
    if selected_city_from_js:
        final_filtered_reviews = [r for r in final_filtered_reviews if r.get('city') == selected_city_from_js]
    # 3. If selected_restaurant_name is provided, further filter
    if selected_restaurant_from_js:
        final_filtered_reviews = [r for r in final_filtered_reviews if r.get('ui_display_name') == selected_restaurant_from_js]
    
    # 2.a.iii. Process final_filtered_reviews for charts and stats
    top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data = process_review_data(final_filtered_reviews)
    total_displayed_reviews = len(final_filtered_reviews)
    
    return jsonify({
        'top_pros': top_pros, 'top_cons': top_cons,
        'average_restaurant_ratings': average_restaurant_ratings,
        'reviews_over_time_chart_data': reviews_over_time_chart_data,
        'total_displayed_reviews': total_displayed_reviews,
        'restaurant_names_for_dropdown': restaurant_names_for_dropdown,
        'city_names_for_dropdown': city_names_for_dropdown,
    })

if __name__ == '__main__':
    app.run(debug=True)
