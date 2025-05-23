import pytest
from review_visualizer.app import app, process_review_data 
from datetime import datetime
from collections import Counter
import json # For loading JSON response

# 1.a. Update Sample Data (COMPREHENSIVE_SAMPLE_REVIEWS)
# Ensure geographic spread for bounding box tests.
# Cluster 1: NYC area (approx. lat 40.7, lng -74.0)
# Cluster 2: LA area (approx. lat 34.0, lng -118.2)
# Outlier: London (approx. lat 51.5, lng -0.1)
COMPREHENSIVE_SAMPLE_REVIEWS = [
    # Restaurant A in City X (NYC)
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 5, 'review_pros': ['Great food XA'], 'review_datetime': datetime(2023, 1, 10), 'latitude': 40.7128, 'longitude': -74.0060}, # NYC
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Good service XA'], 'review_datetime': datetime(2023, 1, 11), 'latitude': 40.7128, 'longitude': -74.0060}, # NYC
    # Restaurant A in City Y (LA) - Disambiguated
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 3, 'review_pros': ['Nice view YA'], 'review_datetime': datetime(2023, 2, 10), 'latitude': 34.0522, 'longitude': -118.2437}, # LA
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 2, 'review_pros': ['Okay YA'], 'review_datetime': datetime(2023, 2, 12), 'latitude': 34.0522, 'longitude': -118.2437}, # LA
    # Restaurant B in City X (NYC)
    {'display_name': 'Restaurant B', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Tasty XB'], 'review_datetime': datetime(2023, 1, 12), 'latitude': 40.7589, 'longitude': -73.9851}, # NYC (near Times Square)
    # Restaurant C in City Z (London) - Outlier
    {'display_name': 'Restaurant C', 'city': 'City Z', 'review_rating': 5, 'review_pros': ['Unique ZC'], 'review_datetime': datetime(2023, 3, 1), 'latitude': 51.5074, 'longitude': -0.1278}, # London
    # Restaurant D - no city, but in NYC geo range
    {'display_name': 'Restaurant D', 'city': None, 'review_rating': 2, 'review_pros': ['No city D'], 'review_datetime': datetime(2023, 3, 5), 'latitude': 40.7300, 'longitude': -73.9900}, # NYC
    # Restaurant E - empty string city, but in LA geo range
    {'display_name': 'Restaurant E', 'city': '', 'review_rating': 3, 'review_pros': ['Empty city E'], 'review_datetime': datetime(2023, 3, 10), 'latitude': 34.0000, 'longitude': -118.2000}, # LA
    # Restaurant F in City X (NYC)
    {'display_name': 'Restaurant F', 'city': 'City X', 'review_rating': 3, 'review_pros': ['New XF'], 'review_datetime': datetime(2023, 4, 1), 'latitude': 40.7000, 'longitude': -74.0100}, # NYC
    # Restaurant G in City Y (LA)
    {'display_name': 'Restaurant G', 'city': 'City Y', 'review_rating': 4.5, 'review_pros': ['Good G'], 'review_datetime': datetime(2023, 4, 5), 'latitude': 34.0500, 'longitude': -118.2400}, # LA
]


@pytest.fixture
def client_with_data(monkeypatch):
    app.config['TESTING'] = True
    captured_contexts = [] # To store contexts from render_template

    # Helper to inject data into MockBigQueryClient
    def _get_client_with_mock_data(data):
        mock_bq_client = MockBigQueryClient(data)
        # Ensure the mock client is set on app.config directly for app-level access if needed
        app.config['BIGQUERY_CLIENT'] = mock_bq_client 
        return app.test_client()

    # Capture context helper
    original_render_template = app.jinja_env.globals.get('render_template')
    def mock_render_template_capture(template_name_or_list, **context):
        captured_contexts.append(context)
        if original_render_template:
            return original_render_template(template_name_or_list, **context)
        return f"Mocked render_template for {template_name_or_list}"

    monkeypatch.setattr(app.jinja_env, 'globals', {**app.jinja_env.globals, 'render_template': mock_render_template_capture})
    
    yield _get_client_with_mock_data, captured_contexts
    
    # Teardown
    if original_render_template:
         monkeypatch.setattr(app.jinja_env, 'globals', {**app.jinja_env.globals, 'render_template': original_render_template})
    else:
        new_globals = app.jinja_env.globals.copy()
        if 'render_template' in new_globals: del new_globals['render_template']
        monkeypatch.setattr(app.jinja_env, 'globals', new_globals)


class MockBigQueryClient:
    def __init__(self, data_to_return):
        self.data_to_return = data_to_return
    def query(self, query_string):
        class MockQueryJob:
            def __init__(self, data):
                self.data = data
            def result(self):
                return self.data
        return MockQueryJob(self.data_to_return)

def augment_reviews_with_ui_name(reviews_raw):
    name_to_cities = {}
    for review in reviews_raw:
        display_name, city = review.get('display_name'), review.get('city')
        if display_name and city: name_to_cities.setdefault(display_name, set()).add(city)
    names_needing_disambiguation = {name for name, cities in name_to_cities.items() if len(cities) > 1}
    augmented_reviews = []
    for review in reviews_raw:
        r_copy = review.copy()
        original_display_name, city = r_copy.get('display_name'), r_copy.get('city')
        r_copy['ui_display_name'] = f"{original_display_name} ({city})" if original_display_name in names_needing_disambiguation and city else original_display_name
        augmented_reviews.append(r_copy)
    return augmented_reviews

# --- Existing Tests (abbreviated for focus, assumed to be present) ---
def test_index_route_context_and_basic_logic(client_with_data): # Example, ensure it uses COMPREHENSIVE_SAMPLE_REVIEWS
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)
    response = client.get('/')
    assert response.status_code == 200
    assert len(captured_contexts) == 1
    context = captured_contexts[0]
    assert context['total_displayed_reviews'] == len(COMPREHENSIVE_SAMPLE_REVIEWS)


# --- New Tests for /update_dashboard_by_map_bounds ---

# 1.b.i. Test for missing/invalid parameters
def test_map_bounds_endpoint_missing_params(client_with_data):
    get_client, _ = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS) # Data doesn't matter much here

    response = client.get('/update_dashboard_by_map_bounds?ne_lat=10') # Missing other params
    assert response.status_code == 400
    json_data = json.loads(response.data)
    assert 'error' in json_data
    assert 'Missing or invalid geographic bounds' in json_data['error']

    response = client.get('/update_dashboard_by_map_bounds?ne_lat=10&ne_lng=10&sw_lat=abc&sw_lng=0') # Invalid param
    assert response.status_code == 400
    json_data = json.loads(response.data)
    assert 'Missing or invalid geographic bounds' in json_data['error']

# 1.b.ii. Test geographic filtering
def test_map_bounds_endpoint_filters_correctly(client_with_data):
    get_client, _ = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    # Bounding Box 1: NYC Area (should include 5 reviews: A(2), B(1), D(1), F(1))
    # Approx bounds: Lat (40.6 to 40.8), Lng (-74.05 to -73.95)
    nyc_bounds = {'ne_lat': 40.8, 'ne_lng': -73.95, 'sw_lat': 40.6, 'sw_lng': -74.05}
    response_nyc = client.get('/update_dashboard_by_map_bounds', query_string=nyc_bounds)
    assert response_nyc.status_code == 200
    data_nyc = json.loads(response_nyc.data)
    assert data_nyc['total_displayed_reviews'] == 5
    nyc_restaurant_ui_names = {r['ui_display_name'] for r in augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS) if 40.6 <= r['latitude'] <= 40.8 and -74.05 <= r['longitude'] <= -73.95}
    assert sorted(data_nyc['restaurant_names_for_dropdown']) == sorted(list(nyc_restaurant_ui_names))
    assert len(data_nyc['average_restaurant_ratings']) == len(nyc_restaurant_ui_names) # Ensure ratings match dropdown

    # Bounding Box 2: LA Area (should include 4 reviews: A(2), E(1), G(1))
    # Approx bounds: Lat (33.9 to 34.1), Lng (-118.3 to -118.1)
    la_bounds = {'ne_lat': 34.1, 'ne_lng': -118.1, 'sw_lat': 33.9, 'sw_lng': -118.3}
    response_la = client.get('/update_dashboard_by_map_bounds', query_string=la_bounds)
    assert response_la.status_code == 200
    data_la = json.loads(response_la.data)
    assert data_la['total_displayed_reviews'] == 4
    la_restaurant_ui_names = {r['ui_display_name'] for r in augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS) if 33.9 <= r['latitude'] <= 34.1 and -118.3 <= r['longitude'] <= -118.1}
    assert sorted(data_la['restaurant_names_for_dropdown']) == sorted(list(la_restaurant_ui_names))

    # Bounding Box 3: Excludes all data (e.g., middle of ocean)
    ocean_bounds = {'ne_lat': 0, 'ne_lng': 0, 'sw_lat': -1, 'sw_lng': -1}
    response_ocean = client.get('/update_dashboard_by_map_bounds', query_string=ocean_bounds)
    assert response_ocean.status_code == 200
    data_ocean = json.loads(response_ocean.data)
    assert data_ocean['total_displayed_reviews'] == 0
    assert data_ocean['restaurant_names_for_dropdown'] == []
    assert data_ocean['city_names_for_dropdown'] == []
    assert data_ocean['average_restaurant_ratings'] == {}

    # Bounding Box 4: Includes all data (very large box)
    all_bounds = {'ne_lat': 90, 'ne_lng': 180, 'sw_lat': -90, 'sw_lng': -180}
    response_all = client.get('/update_dashboard_by_map_bounds', query_string=all_bounds)
    assert response_all.status_code == 200
    data_all = json.loads(response_all.data)
    assert data_all['total_displayed_reviews'] == len(COMPREHENSIVE_SAMPLE_REVIEWS)


# 1.b.iii. Test combined geographic and dropdown filters
def test_map_bounds_endpoint_with_dropdown_filters(client_with_data):
    get_client, _ = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    # NYC Area bounds
    nyc_bounds = {'ne_lat': 40.8, 'ne_lng': -73.95, 'sw_lat': 40.6, 'sw_lng': -74.05}
    test_city_in_nyc = 'City X' # Has A, B, F in NYC
    test_restaurant_in_nyc_city_x_ui_name = 'Restaurant A (City X)' # 2 reviews in NYC
    test_restaurant_b_in_nyc_city_x_ui_name = 'Restaurant B' # 1 review in NYC, not disambiguated

    # Scenario 1: Bounds + City
    params_city = {**nyc_bounds, 'selected_city': test_city_in_nyc}
    response_city = client.get('/update_dashboard_by_map_bounds', query_string=params_city)
    assert response_city.status_code == 200
    data_city = json.loads(response_city.data)
    # Expected in NYC & City X: A(2), B(1), F(1) = 4 reviews
    assert data_city['total_displayed_reviews'] == 4 
    # Check restaurant names in dropdown are only those from City X within NYC bounds
    expected_restaurants_nyc_city_x = ['Restaurant A (City X)', 'Restaurant B', 'Restaurant F']
    assert sorted(data_city['restaurant_names_for_dropdown']) == sorted(expected_restaurants_nyc_city_x)
    # Check city names in dropdown (should only be City X as it's selected and others are filtered out by geo then city)
    assert data_city['city_names_for_dropdown'] == [test_city_in_nyc] # Only selected city if it has results

    # Scenario 2: Bounds + Restaurant (Restaurant A (City X))
    params_rest_a = {**nyc_bounds, 'selected_restaurant_name': test_restaurant_in_nyc_city_x_ui_name}
    response_rest_a = client.get('/update_dashboard_by_map_bounds', query_string=params_rest_a)
    assert response_rest_a.status_code == 200
    data_rest_a = json.loads(response_rest_a.data)
    assert data_rest_a['total_displayed_reviews'] == 2 # Only Restaurant A (City X)
    # Dropdown for restaurants should reflect all in NYC bounds, as city filter isn't active from dropdowns here
    # But average_restaurant_ratings should be only for Restaurant A (City X)
    assert len(data_rest_a['average_restaurant_ratings']) == 1
    assert test_restaurant_in_nyc_city_x_ui_name in data_rest_a['average_restaurant_ratings']
    
    # Scenario 3: Bounds + City + Restaurant
    params_both = {**nyc_bounds, 'selected_city': test_city_in_nyc, 'selected_restaurant_name': test_restaurant_in_nyc_city_x_ui_name}
    response_both = client.get('/update_dashboard_by_map_bounds', query_string=params_both)
    assert response_both.status_code == 200
    data_both = json.loads(response_both.data)
    assert data_both['total_displayed_reviews'] == 2 # Restaurant A (City X)
    assert len(data_both['average_restaurant_ratings']) == 1
    assert test_restaurant_in_nyc_city_x_ui_name in data_both['average_restaurant_ratings']
    # Restaurant dropdown should only list restaurants in City X (due to selected_city)
    assert sorted(data_both['restaurant_names_for_dropdown']) == sorted(expected_restaurants_nyc_city_x)
    # City dropdown should only list City X
    assert data_both['city_names_for_dropdown'] == [test_city_in_nyc]

    # Scenario 4: Bounds + City + different Restaurant in that City
    params_city_rest_b = {**nyc_bounds, 'selected_city': test_city_in_nyc, 'selected_restaurant_name': test_restaurant_b_in_nyc_city_x_ui_name}
    response_city_rest_b = client.get('/update_dashboard_by_map_bounds', query_string=params_city_rest_b)
    assert response_city_rest_b.status_code == 200
    data_city_rest_b = json.loads(response_city_rest_b.data)
    assert data_city_rest_b['total_displayed_reviews'] == 1 # Restaurant B in City X
    assert len(data_city_rest_b['average_restaurant_ratings']) == 1
    assert test_restaurant_b_in_nyc_city_x_ui_name in data_city_rest_b['average_restaurant_ratings']


# Ensure other tests are present and use appropriate sample data
def test_process_review_data_with_disambiguation(): # Example of an existing test
    augmented_data = augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS)
    data_for_ra_city_x = [r for r in augmented_data if r['ui_display_name'] == 'Restaurant A (City X)']
    top_pros, top_cons, avg_ratings, time_series = process_review_data(data_for_ra_city_x)
    assert 'Restaurant A (City X)' in avg_ratings
    assert avg_ratings['Restaurant A (City X)'] == 4.5

def test_index_route_with_empty_bq_data(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client([]) 
    response = client.get('/')
    assert response.status_code == 200
    context = captured_contexts[-1]
    assert context['total_displayed_reviews'] == 0
    assert b"No pros data available" in response.data
```
