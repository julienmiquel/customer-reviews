import pytest
from review_visualizer.app import app, process_review_data # Assuming app.py is in review_visualizer
from datetime import datetime
from collections import Counter

@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['BIGQUERY_CLIENT'] = None 
    with app.test_client() as client:
        yield client

# Extended Sample data for testing map data preparation, including lat/lng variations
SAMPLE_REVIEWS_FOR_MAP = [
    { # Restaurant A - first entry with valid lat/lng
        'display_name': 'Restaurant A', 'review_rating': 5, 'review_pros': ['Great food'], 'review_cons': [],
        'review_text': 'Loved it!', 'review_datetime': datetime(2023, 1, 10), 'latitude': 40.7128, 'longitude': -74.0060
    },
    { # Restaurant B - valid lat/lng
        'display_name': 'Restaurant B', 'review_rating': 4, 'review_pros': [], 'review_cons': [],
        'review_text': 'Pretty good.', 'review_datetime': datetime(2023, 1, 15), 'latitude': 34.0522, 'longitude': -118.2437
    },
    { # Restaurant A - second entry, different rating, should use first lat/lng
        'display_name': 'Restaurant A', 'review_rating': 3, 'review_pros': [], 'review_cons': [],
        'review_text': 'Okay.', 'review_datetime': datetime(2023, 2, 5), 'latitude': 40.7129, 'longitude': -74.0061 # Slightly different lat/lng
    },
    { # Restaurant C - review with invalid lat (None)
        'display_name': 'Restaurant C', 'review_rating': 2, 'review_pros': [], 'review_cons': [],
        'review_text': 'Not good.', 'review_datetime': datetime(2023, 2, 20), 'latitude': None, 'longitude': -73.9851
    },
    { # Restaurant C - review with valid lat/lng (this one should be picked)
        'display_name': 'Restaurant C', 'review_rating': 4, 'review_pros': [], 'review_cons': [],
        'review_text': 'Much better!', 'review_datetime': datetime(2023, 3, 1), 'latitude': 40.7580, 'longitude': -73.9855 
    },
     { # Restaurant D - review with invalid lng (string)
        'display_name': 'Restaurant D', 'review_rating': 1, 'review_pros': [], 'review_cons': [],
        'review_text': 'Bad.', 'review_datetime': datetime(2023, 3, 5), 'latitude': 40.7000, 'longitude': "invalid_lng"
    },
    { # Restaurant E - no lat/lng fields at all
        'display_name': 'Restaurant E', 'review_rating': 5, 'review_pros': [], 'review_cons': [],
        'review_text': 'Excellent!', 'review_datetime': datetime(2023, 3, 10)
    },
    { # Restaurant D - another review, but still no valid lat/lng for D because the first was invalid
        'display_name': 'Restaurant D', 'review_rating': 3, 'review_pros': [], 'review_cons': [],
        'review_text': 'Okay now.', 'review_datetime': datetime(2023, 3, 15), 'latitude': 40.7001, 'longitude': -73.9001
    },
    { # No display name
        'display_name': None, 'review_rating': 5, 'review_pros': [], 'review_cons': [],
        'review_text': 'No name!', 'review_datetime': datetime(2023, 1, 1), 'latitude': 40.0, 'longitude': -74.0
    }
]
# Simpler sample for process_review_data tests not focused on map
SAMPLE_REVIEWS_SIMPLE = [
    {
        'display_name': 'Restaurant A', 'review_rating': 5, 
        'review_pros': ['Great food', 'Friendly staff'], 'review_cons': ['A bit pricey'],
        'review_text': 'Loved it!', 'review_datetime': datetime(2023, 1, 10)
    },
    {
        'display_name': 'Restaurant B', 'review_rating': 4, 
        'review_pros': ['Good portions'], 'review_cons': None,
        'review_text': 'Pretty good.', 'review_datetime': datetime(2023, 1, 15)
    }
]


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

def test_index_route_loads(client, monkeypatch):
    monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': MockBigQueryClient(SAMPLE_REVIEWS_SIMPLE)})
    response = client.get('/')
    assert response.status_code == 200
    assert b"<title>Review Visualizer</title>" in response.data

def test_process_review_data_empty():
    top_pros, top_cons, avg_ratings, time_series = process_review_data([])
    assert top_pros == []
    assert top_cons == []
    assert avg_ratings == {}
    assert time_series == {}

def test_process_review_data_aggregation_simple(): # Using simple sample
    top_pros, top_cons, avg_ratings, time_series = process_review_data(SAMPLE_REVIEWS_SIMPLE)
    assert ('great food', 1) in top_pros
    assert ('friendly staff', 1) in top_pros
    assert ('good portions', 1) in top_pros
    assert len(top_pros) == 3
    assert ('a bit pricey', 1) in top_cons
    assert avg_ratings['Restaurant A'] == 5.0
    assert avg_ratings['Restaurant B'] == 4.0

# ... (other existing tests like test_filtering_logic_with_selection, etc., assumed to be here)


def test_restaurants_map_data_preparation(client, monkeypatch):
    """
    Tests the logic that prepares `restaurants_map_data` as used in the index route.
    It simulates the data aggregation steps from `app.py` and compares against 
    the data made available to the template when the route is called.
    """
    # 1.c.i. Manually simulate the expected data generation
    expected_unique_restaurants_info = {}
    for review in SAMPLE_REVIEWS_FOR_MAP:
        name = review.get('display_name')
        lat = review.get('latitude')
        lng = review.get('longitude')
        if name and name not in expected_unique_restaurants_info and lat is not None and lng is not None:
            try:
                # Ensure lat/lng are floats
                expected_unique_restaurants_info[name] = {'lat': float(lat), 'lng': float(lng)}
            except (ValueError, TypeError):
                pass # Skip if lat/lng cannot be parsed to float

    expected_all_restaurant_aggregates = {}
    for review in SAMPLE_REVIEWS_FOR_MAP:
        name = review.get('display_name')
        rating = review.get('review_rating')
        if name and rating is not None:
            if name not in expected_all_restaurant_aggregates:
                expected_all_restaurant_aggregates[name] = {'total_rating': 0, 'count': 0}
            try:
                expected_all_restaurant_aggregates[name]['total_rating'] += float(rating)
                expected_all_restaurant_aggregates[name]['count'] += 1
            except (ValueError, TypeError):
                pass

    expected_all_average_ratings = {}
    for name, data in expected_all_restaurant_aggregates.items():
        if data['count'] > 0:
            expected_all_average_ratings[name] = round(data['total_rating'] / data['count'], 2)
        else:
            expected_all_average_ratings[name] = 0
            
    expected_map_data = []
    for name, info in expected_unique_restaurants_info.items():
        expected_map_data.append({
            'name': name,
            'lat': info['lat'],
            'lng': info['lng'],
            'avg_rating': expected_all_average_ratings.get(name, 0),
            'review_count': expected_all_restaurant_aggregates.get(name, {}).get('count', 0)
        })
    # Sort for consistent comparison
    expected_map_data = sorted(expected_map_data, key=lambda x: x['name'])

    # 1.c.ii. Get actual data by calling the route
    # The MockBigQueryClient will return SAMPLE_REVIEWS_FOR_MAP
    monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': MockBigQueryClient(SAMPLE_REVIEWS_FOR_MAP)})
    
    # To get the context directly, we need a bit more work or a flask-specific testing utility.
    # For now, we'll add a temporary way to grab the context in the app for this test.
    actual_map_data_from_context = None
    
    @app.before_request
    def before_request_func():
        # This is a bit of a hack for testing. In a real app, you wouldn't do this.
        # It makes the data available outside the direct response.
        # A better way might involve a custom signal or a specific test setup for contexts.
        if app.config['TESTING'] and hasattr(app, '_test_render_context'):
            delattr(app, '_test_render_context')

    # Temporarily modify render_template to capture its context for testing
    original_render_template = app.jinja_env.globals['render_template']
    def mock_render_template(template_name_or_list, **context):
        if app.config['TESTING']:
            app._test_render_context = context # Store context
        return original_render_template(template_name_or_list, **context)
    
    app.jinja_env.globals['render_template'] = mock_render_template
    
    client.get('/') # Call the route to populate the context

    if hasattr(app, '_test_render_context'):
        actual_map_data_from_context = app._test_render_context.get('restaurants_map_data', [])
    
    # Restore original render_template
    app.jinja_env.globals['render_template'] = original_render_template
    if hasattr(app, '_test_render_context'): # Clean up
        delattr(app, '_test_render_context')


    # 1.c.iii. Assert
    assert actual_map_data_from_context is not None, "restaurants_map_data not found in template context"
    actual_map_data_sorted = sorted(actual_map_data_from_context, key=lambda x: x['name'])
    
    # Assertions for each field
    assert len(actual_map_data_sorted) == len(expected_map_data), "Number of restaurants in map data differs"

    for actual, expected in zip(actual_map_data_sorted, expected_map_data):
        assert actual['name'] == expected['name']
        assert actual['lat'] == expected['lat']
        assert actual['lng'] == expected['lng']
        assert actual['avg_rating'] == expected['avg_rating']
        assert actual['review_count'] == expected['review_count']

    # 1.d. Edge Case Validations (covered by the expected_map_data construction logic):
    # Restaurant A: Uses first lat/lng (40.7128, -74.0060). Avg rating (5+3)/2=4.0. Count 2.
    res_a_actual = next((r for r in actual_map_data_sorted if r['name'] == 'Restaurant A'), None)
    assert res_a_actual is not None
    assert res_a_actual['lat'] == 40.7128
    assert res_a_actual['avg_rating'] == 4.0
    assert res_a_actual['review_count'] == 2

    # Restaurant B: Simple case. Avg rating 4.0. Count 1.
    res_b_actual = next((r for r in actual_map_data_sorted if r['name'] == 'Restaurant B'), None)
    assert res_b_actual is not None
    assert res_b_actual['lat'] == 34.0522
    assert res_b_actual['avg_rating'] == 4.0
    assert res_b_actual['review_count'] == 1

    # Restaurant C: First review invalid lat, second valid (40.7580, -73.9855). Avg rating (2+4)/2=3.0. Count 2.
    res_c_actual = next((r for r in actual_map_data_sorted if r['name'] == 'Restaurant C'), None)
    assert res_c_actual is not None
    assert res_c_actual['lat'] == 40.7580 
    assert res_c_actual['lng'] == -73.9855
    assert res_c_actual['avg_rating'] == 3.0
    assert res_c_actual['review_count'] == 2
    
    # Restaurant D: First review invalid lng ("invalid_lng"). Should not be in map_data as no valid coords were found first.
    res_d_actual = next((r for r in actual_map_data_sorted if r['name'] == 'Restaurant D'), None)
    assert res_d_actual is None, "Restaurant D should not be in map_data due to invalid initial lng"

    # Restaurant E: No lat/lng fields at all. Should not be in map_data.
    res_e_actual = next((r for r in actual_map_data_sorted if r['name'] == 'Restaurant E'), None)
    assert res_e_actual is None, "Restaurant E should not be in map_data as it has no lat/lng"
    
    # Number of unique restaurants in map_data (A, B, C)
    assert len(actual_map_data_sorted) == 3


# Existing tests for filtering, etc. should be here
def test_filtering_logic_with_selection(client, monkeypatch):
    monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': MockBigQueryClient(SAMPLE_REVIEWS_FOR_MAP)}) # Use map sample
    response = client.get('/?selected_restaurant_name=Restaurant A')
    assert response.status_code == 200
    assert b'<option value="Restaurant A" selected>Restaurant A</option>' in response.data
    # Check if pros/cons in the chart data are filtered based on "Restaurant A" from SAMPLE_REVIEWS_FOR_MAP
    # Restaurant A pros: ['Great food'] (appears twice in its own reviews)
    # Response data will contain the chart script data.
    # This is an indirect check. Direct check of `process_review_data` is better for this.
    assert b"great food" in response.data 
    # Restaurant B pros: e.g. 'Good portions' - should not be in the chart data if filtered to A
    # This requires inspecting the chart data specifically.
    # For now, this test primarily checks if the filter selection is maintained.

def test_filtering_logic_all_restaurants(client, monkeypatch):
    monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': MockBigQueryClient(SAMPLE_REVIEWS_FOR_MAP)})
    response = client.get('/?selected_restaurant_name=')
    assert response.status_code == 200
    assert b'<option value="" selected>All Restaurants</option>' in response.data
    # Check if a pro from a restaurant other than A appears, indicating all data used for charts
    # e.g., Restaurant B has 'Good portions' in SAMPLE_REVIEWS_SIMPLE. SAMPLE_REVIEWS_FOR_MAP is simpler for pros.
    # Restaurant B in SAMPLE_REVIEWS_FOR_MAP has no specific pros listed in the array, so it won't show up.
    # Let's check for "Restaurant B" itself in the ratings chart data for instance.
    assert b"Restaurant B" in response.data


def test_index_route_with_empty_bq_data(client, monkeypatch):
    monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': MockBigQueryClient([])})
    response = client.get('/')
    assert response.status_code == 200
    assert b"No pros data available for the current selection." in response.data
    assert b"No individual reviews to display for the current selection." in response.data
    assert b"No restaurant location data available to display on map." in response.data # Check map specific message
    
    # Check context if possible (using the hacky method for this specific test if needed, or rely on HTML)
    # For this test, checking HTML is sufficient given the other direct tests of data processing.
    assert b"All Restaurants" in response.data # Filter should still be there

    # Clean up app context hack if it was applied by another test and not cleaned up
    if hasattr(app, '_test_render_context'):
        delattr(app, '_test_render_context')
    if hasattr(app.jinja_env.globals, '_original_render_template'): # if we stored it
        app.jinja_env.globals['render_template'] = app.jinja_env.globals['_original_render_template']
        delattr(app.jinja_env.globals, '_original_render_template')

# It's good practice to also ensure __init__.py exists in tests and review_visualizer folder
# touch review_visualizer/__init__.py
# touch review_visualizer/tests/__init__.py
# (These commands would be run in bash, not part of the Python file)

# To run: python -m pytest review_visualizer/tests/test_app.py
# (from /app directory)
