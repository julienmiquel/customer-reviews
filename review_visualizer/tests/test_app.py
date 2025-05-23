import pytest
from review_visualizer.app import app, process_review_data 
from datetime import datetime
from collections import Counter

# 1.a. Update Sample Data (COMPREHENSIVE_SAMPLE_REVIEWS)
COMPREHENSIVE_SAMPLE_REVIEWS = [
    # Restaurant A in City X
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 5, 'review_pros': ['Great food XA'], 'review_datetime': datetime(2023, 1, 10), 'latitude': 1.0, 'longitude': 1.0},
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Good service XA'], 'review_datetime': datetime(2023, 1, 11), 'latitude': 1.0, 'longitude': 1.0},
    # Restaurant A in City Y
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 3, 'review_pros': ['Nice view YA'], 'review_datetime': datetime(2023, 2, 10), 'latitude': 2.0, 'longitude': 2.0},
    # Restaurant B in City X
    {'display_name': 'Restaurant B', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Tasty XB'], 'review_datetime': datetime(2023, 1, 12), 'latitude': 1.1, 'longitude': 1.1},
    # Restaurant C in City Z (no other restaurants in City Z for some tests)
    {'display_name': 'Restaurant C', 'city': 'City Z', 'review_rating': 5, 'review_pros': ['Unique ZC'], 'review_datetime': datetime(2023, 3, 1), 'latitude': 3.0, 'longitude': 3.0},
    # Restaurant D - no city
    {'display_name': 'Restaurant D', 'city': None, 'review_rating': 2, 'review_pros': ['No city D'], 'review_datetime': datetime(2023, 3, 5), 'latitude': 4.0, 'longitude': 4.0},
    # Restaurant E - empty string city
    {'display_name': 'Restaurant E', 'city': '', 'review_rating': 3, 'review_pros': ['Empty city E'], 'review_datetime': datetime(2023, 3, 10), 'latitude': 5.0, 'longitude': 5.0},
    # Another review for Restaurant A in City Y to test aggregation
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 2, 'review_pros': ['Okay YA'], 'review_datetime': datetime(2023, 2, 12), 'latitude': 2.0, 'longitude': 2.0},
    # Restaurant F in City X (another one in City X)
    {'display_name': 'Restaurant F', 'city': 'City X', 'review_rating': 3, 'review_pros': ['New XF'], 'review_datetime': datetime(2023, 4, 1), 'latitude': 1.2, 'longitude': 1.2},
]

# 1.b. Fixture for providing app client and mock BQ client
@pytest.fixture
def client_with_data(monkeypatch):
    app.config['TESTING'] = True
    
    # Helper to inject data into MockBigQueryClient
    def _get_client_with_mock_data(data):
        mock_bq_client = MockBigQueryClient(data)
        monkeypatch.setattr(app, 'config', {**app.config, 'BIGQUERY_CLIENT': mock_bq_client})
        return app.test_client()

    # Capture context helper
    original_render_template = app.jinja_env.globals.get('render_template')
    captured_contexts = []
    def mock_render_template(template_name_or_list, **context):
        captured_contexts.append(context)
        if original_render_template:
            return original_render_template(template_name_or_list, **context)
        return f"Mocked render_template for {template_name_or_list}" # Fallback if original is None

    monkeypatch.setattr(app.jinja_env, 'globals', {**app.jinja_env.globals, 'render_template': mock_render_template})
    
    # Yield a function that can set data per test, and the capture list
    yield _get_client_with_mock_data, captured_contexts
    
    # Teardown: Restore original render_template if it was patched
    if original_render_template:
         monkeypatch.setattr(app.jinja_env, 'globals', {**app.jinja_env.globals, 'render_template': original_render_template})
    else: # If it was None, remove our mock
        new_globals = app.jinja_env.globals.copy()
        del new_globals['render_template']
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

# --- Helper to augment data similar to app.py for testing process_review_data ---
def augment_reviews_with_ui_name(reviews_raw):
    name_to_cities = {}
    for review in reviews_raw:
        display_name = review.get('display_name')
        city = review.get('city')
        if display_name and city: # Only consider if both exist
            name_to_cities.setdefault(display_name, set()).add(city)
    
    names_needing_disambiguation = {name for name, cities in name_to_cities.items() if len(cities) > 1}
    
    augmented_reviews = []
    for review in reviews_raw:
        r_copy = review.copy()
        original_display_name = r_copy.get('display_name')
        city = r_copy.get('city')
        if original_display_name in names_needing_disambiguation and city: # Must have city to be disambiguated
            r_copy['ui_display_name'] = f"{original_display_name} ({city})"
        else:
            r_copy['ui_display_name'] = original_display_name # Handles None city or non-ambiguous names
        augmented_reviews.append(r_copy)
    return augmented_reviews

# 1.c. Test index Route Context and Basic Logic
def test_index_route_context_and_basic_logic(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)
    
    response = client.get('/')
    assert response.status_code == 200
    
    assert len(captured_contexts) == 1
    context = captured_contexts[0]

    assert 'city_names' in context
    assert context['city_names'] == ['City X', 'City Y', 'City Z'] # Sorted, unique, non-empty cities
    
    assert 'selected_city' in context
    assert context['selected_city'] == '' # Default
    
    assert 'total_displayed_reviews' in context
    # All 9 reviews are valid for display initially (None city and empty city are still processed)
    assert context['total_displayed_reviews'] == 9 
    
    assert 'total_distinct_cities' in context
    assert context['total_distinct_cities'] == 3 # City X, City Y, City Z

    assert 'restaurant_names' in context # These are ui_display_names
    expected_ui_names = [
        'Restaurant A (City X)', 'Restaurant A (City Y)', 
        'Restaurant B', # Only in City X, not disambiguated
        'Restaurant C', # Only in City Z, not disambiguated
        'Restaurant D', # No city, not disambiguated
        'Restaurant E', # Empty city, not disambiguated
        'Restaurant F'  # Only in City X, not disambiguated
    ]
    assert sorted(context['restaurant_names']) == sorted(expected_ui_names)

# 1.d. Test process_review_data with ui_display_name
def test_process_review_data_with_disambiguation():
    augmented_data = augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS)
    
    # Filter for Restaurant A (City X) to test specific aggregation
    data_for_ra_city_x = [r for r in augmented_data if r['ui_display_name'] == 'Restaurant A (City X)']
    
    top_pros, top_cons, avg_ratings, time_series = process_review_data(data_for_ra_city_x)
    
    assert 'Restaurant A (City X)' in avg_ratings
    assert avg_ratings['Restaurant A (City X)'] == (5 + 4) / 2 # 4.5
    assert len(avg_ratings) == 1
    
    # Test pros for Restaurant A (City X)
    # ('great food xa', 1), ('good service xa', 1)
    assert any(p[0] == 'great food xa' for p in top_pros)
    assert any(p[0] == 'good service xa' for p in top_pros)

    # Test with all augmented data
    top_pros_all, _, avg_ratings_all, _ = process_review_data(augmented_data)
    assert 'Restaurant A (City X)' in avg_ratings_all
    assert avg_ratings_all['Restaurant A (City X)'] == 4.5
    assert 'Restaurant A (City Y)' in avg_ratings_all
    assert avg_ratings_all['Restaurant A (City Y)'] == (3 + 2) / 2 # 2.5
    assert 'Restaurant B' in avg_ratings_all # No (City X) as it's unique
    assert avg_ratings_all['Restaurant B'] == 4.0


# 1.e. Test Filtering Scenarios
def test_filter_city_only(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    response = client.get('/?selected_city=City X')
    assert response.status_code == 200
    context = captured_contexts[-1] # Get the latest context

    assert context['selected_city'] == 'City X'
    # Reviews in City X: A (2), B (1), F (1) = 4
    assert context['total_displayed_reviews'] == 4 
    
    expected_restaurant_names_in_city_x = ['Restaurant A (City X)', 'Restaurant B', 'Restaurant F']
    assert sorted(context['restaurant_names']) == sorted(expected_restaurant_names_in_city_x)
    
    assert 'Restaurant A (City X)' in context['average_restaurant_ratings']
    assert 'Restaurant B' in context['average_restaurant_ratings'] # Not B (City X)
    assert 'Restaurant F' in context['average_restaurant_ratings']
    assert 'Restaurant A (City Y)' not in context['average_restaurant_ratings']

def test_filter_disambiguated_restaurant_only(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    # Restaurant A (City Y) has 2 reviews. Original name "Restaurant A", city "City Y"
    response = client.get('/?selected_restaurant_name=Restaurant A (City Y)')
    assert response.status_code == 200
    context = captured_contexts[-1]

    assert context['selected_restaurant_name'] == 'Restaurant A (City Y)'
    assert context['total_displayed_reviews'] == 2
    
    # average_restaurant_ratings should only contain 'Restaurant A (City Y)'
    assert len(context['average_restaurant_ratings']) == 1
    assert 'Restaurant A (City Y)' in context['average_restaurant_ratings']
    assert context['average_restaurant_ratings']['Restaurant A (City Y)'] == 2.5 # (3+2)/2

def test_filter_combined_city_and_restaurant(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    # Select "City X" and "Restaurant A (City X)"
    # (Restaurant A is in City X and City Y, Restaurant B is only in City X)
    response = client.get('/?selected_city=City X&selected_restaurant_name=Restaurant A (City X)')
    assert response.status_code == 200
    context = captured_contexts[-1]

    assert context['selected_city'] == 'City X'
    assert context['selected_restaurant_name'] == 'Restaurant A (City X)'
    assert context['total_displayed_reviews'] == 2 # Restaurant A in City X has 2 reviews
    
    # Restaurant dropdown should now only list restaurants in City X
    expected_restaurant_names_in_city_x = ['Restaurant A (City X)', 'Restaurant B', 'Restaurant F']
    assert sorted(context['restaurant_names']) == sorted(expected_restaurant_names_in_city_x)

    # Charts should only reflect "Restaurant A (City X)"
    assert len(context['average_restaurant_ratings']) == 1
    assert 'Restaurant A (City X)' in context['average_restaurant_ratings']
    assert context['average_restaurant_ratings']['Restaurant A (City X)'] == 4.5

def test_filter_all_cities_all_restaurants(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)

    response = client.get('/') # No filters
    assert response.status_code == 200
    context = captured_contexts[-1]

    assert context['selected_city'] == ''
    assert context['selected_restaurant_name'] == ''
    assert context['total_displayed_reviews'] == 9 # All reviews in sample
    
    expected_all_ui_names = [
        'Restaurant A (City X)', 'Restaurant A (City Y)', 
        'Restaurant B', 'Restaurant C', 'Restaurant D', 'Restaurant E', 'Restaurant F'
    ]
    assert sorted(context['restaurant_names']) == sorted(expected_all_ui_names)
    assert len(context['average_restaurant_ratings']) == len(expected_all_ui_names)

# 1.f. Test ui_display_name generation more directly (optional, but good for clarity)
def test_ui_display_name_generation_logic():
    sample = [
        {'display_name': 'BK', 'city': 'Gotham'},
        {'display_name': 'BK', 'city': 'Metropolis'},
        {'display_name': 'Wayne Subs', 'city': 'Gotham'},
        {'display_name': 'Daily Planet Cafe', 'city': None}, # No city
        {'display_name': 'Star Labs Coffee', 'city': ''},    # Empty city
        {'display_name': 'Ace Chemicals', 'city': 'Gotham'}, # Unique name in Gotham
    ]
    augmented = augment_reviews_with_ui_name(sample)
    
    ui_names = {r['ui_display_name'] for r in augmented}
    expected_ui_names = {
        'BK (Gotham)', 
        'BK (Metropolis)', 
        'Wayne Subs', # Not ambiguous
        'Daily Planet Cafe', # Not ambiguous (no city)
        'Star Labs Coffee', # Not ambiguous (empty city)
        'Ace Chemicals' # Not ambiguous
    }
    assert ui_names == expected_ui_names

    # Check specific cases
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'BK' and r['city'] == 'Gotham') == 'BK (Gotham)'
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'Wayne Subs') == 'Wayne Subs'
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'Daily Planet Cafe') == 'Daily Planet Cafe'

# Test map data still uses original names (regression check)
def test_map_data_uses_original_names(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client(COMPREHENSIVE_SAMPLE_REVIEWS)
    client.get('/') # Make a request to populate context
    context = captured_contexts[-1]

    assert 'restaurants_map_data' in context
    map_data_names = {item['name'] for item in context['restaurants_map_data']}
    
    # Expected original names that have valid lat/lng
    expected_original_names_on_map = {'Restaurant A', 'Restaurant B', 'Restaurant C', 'Restaurant F'} 
    # D and E have issues with lat/lng or city in the sample
    
    assert map_data_names == expected_original_names_on_map
    
    # Check one item to ensure structure
    res_a_map_info = next(item for item in context['restaurants_map_data'] if item['name'] == 'Restaurant A')
    assert res_a_map_info['avg_rating'] == ((5+4)/2 + (3+2)/2) / 2 # Avg of (Avg City X) and (Avg City Y)
    # Correction: map data avg_rating is for the original name across all its locations
    # Restaurant A City X: (5+4)/2 = 4.5, count = 2
    # Restaurant A City Y: (3+2)/2 = 2.5, count = 2
    # Overall for Restaurant A: (5+4+3+2) / 4 = 14 / 4 = 3.5
    assert res_a_map_info['avg_rating'] == 3.5
    assert res_a_map_info['review_count'] == 4


# Placeholder for other tests if they were removed for brevity in prompt
def test_index_route_with_empty_bq_data(client_with_data):
    get_client, captured_contexts = client_with_data
    client = get_client([]) # Empty data from BQ
    response = client.get('/')
    assert response.status_code == 200
    context = captured_contexts[-1]
    assert context['total_displayed_reviews'] == 0
    assert context['total_distinct_cities'] == 0
    assert context['city_names'] == []
    assert context['restaurant_names'] == []
    assert b"No pros data available" in response.data
    assert b"No restaurant location data available" in response.data
```
