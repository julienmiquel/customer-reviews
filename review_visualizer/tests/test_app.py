import pytest
from review_visualizer.app import app, process_review_data
from datetime import datetime

@pytest.fixture
def client():
    app.config['TESTING'] = True
    # Disable BigQuery client for tests, as we'll use sample data or mock it.
    # This prevents actual API calls during testing.
    app.config['BIGQUERY_CLIENT'] = None 
    with app.test_client() as client:
        yield client

# Sample data for testing
SAMPLE_REVIEWS = [
    {
        'display_name': 'Restaurant A', 'review_rating': 5, 
        'review_pros': ['Great food', 'Friendly staff'], 'review_cons': ['A bit pricey'],
        'review_text': 'Loved it!', 'review_datetime': datetime(2023, 1, 10)
    },
    {
        'display_name': 'Restaurant B', 'review_rating': 4, 
        'review_pros': ['Good portions'], 'review_cons': None, # Test None cons
        'review_text': 'Pretty good.', 'review_datetime': datetime(2023, 1, 15)
    },
    {
        'display_name': 'Restaurant A', 'review_rating': 3, 
        'review_pros': ['Great food'], 'review_cons': ['Slow service', 'A bit pricey'],
        'review_text': 'Food good, service bad.', 'review_datetime': datetime(2023, 2, 5)
    },
    {
        'display_name': 'Restaurant C', 'review_rating': 2, 
        'review_pros': None, 'review_cons': ['Not clean'], # Test None pros
        'review_text': 'Would not recommend.', 'review_datetime': datetime(2023, 2, 20)
    },
    {
        'display_name': 'Restaurant B', 'review_rating': 5, 
        'review_pros': ['Great food', 'Good portions'], 'review_cons': [], # Test empty list cons
        'review_text': 'Excellent this time!', 'review_datetime': datetime(2023, 3, 1)
    },
     { # Review with string pro/con
        'display_name': 'Restaurant C', 'review_rating': 4,
        'review_pros': 'Very tasty', 'review_cons': 'Small menu',
        'review_text': 'Good but limited.', 'review_datetime': datetime(2023, 3, 5)
    },
    { # Review with no pros/cons fields
        'display_name': 'Restaurant D', 'review_rating': 1,
        'review_text': 'Terrible.', 'review_datetime': datetime(2023, 1, 25)
    }
]

def test_index_route_loads(client, monkeypatch):
    """Test if the index route loads and contains basic expected content."""
    # Mock the BigQuery client within the app's context for this route test
    class MockBigQueryClient:
        def query(self, query_string):
            class MockQueryJob:
                def result(self):
                    return SAMPLE_REVIEWS # Return sample data for route processing
            return MockQueryJob()

    monkeypatch.setattr('review_visualizer.app.bigquery.Client', MockBigQueryClient)
    
    response = client.get('/')
    assert response.status_code == 200
    assert b"<title>Review Visualizer</title>" in response.data
    assert b"Burger King Review Dashboard" in response.data # Main heading

def test_process_review_data_empty():
    """Test data processing with an empty list of reviews."""
    top_pros, top_cons, avg_ratings, time_series = process_review_data([])
    assert top_pros == []
    assert top_cons == []
    assert avg_ratings == {}
    assert time_series == {}

def test_process_review_data_aggregation():
    """Test aggregation of pros, cons, ratings, and time series data."""
    top_pros, top_cons, avg_ratings, time_series = process_review_data(SAMPLE_REVIEWS)

    # Test Pros
    assert ('great food', 3) in top_pros
    assert ('good portions', 2) in top_pros
    assert ('friendly staff', 1) in top_pros
    assert ('very tasty', 1) in top_pros
    assert len(top_pros) == 4 # Based on sample

    # Test Cons
    assert ('a bit pricey', 2) in top_cons
    assert ('slow service', 1) in top_cons
    assert ('not clean', 1) in top_cons
    assert ('small menu', 1) in top_cons
    assert len(top_cons) == 4 # Based on sample

    # Test Average Restaurant Ratings
    assert avg_ratings['Restaurant A'] == (5 + 3) / 2  # 4.0
    assert avg_ratings['Restaurant B'] == (4 + 5) / 2  # 4.5
    assert avg_ratings['Restaurant C'] == (2 + 4) / 2  # 3.0
    assert avg_ratings['Restaurant D'] == 1.0
    assert len(avg_ratings) == 4

    # Test Time Series Data
    assert 'labels' in time_series
    assert 'review_counts' in time_series
    assert 'average_ratings' in time_series
    
    assert time_series['labels'] == ['2023-01', '2023-02', '2023-03']
    # Counts: Jan (A, B, D) = 3, Feb (A, C) = 2, Mar (B, C) = 2
    assert time_series['review_counts'] == [3, 2, 2] 
    # Avg Ratings: 
    # Jan (5+4+1)/3 = 10/3 = 3.33
    # Feb (3+2)/2 = 2.5
    # Mar (5+4)/2 = 4.5
    assert time_series['average_ratings'] == [round(10/3, 2), 2.5, 4.5]


def test_filtering_logic_with_selection(client, monkeypatch):
    """Test the full route logic with a restaurant selected."""
    class MockBigQueryClient:
        def query(self, query_string):
            class MockQueryJob:
                def result(self):
                    return SAMPLE_REVIEWS
            return MockQueryJob()

    monkeypatch.setattr('review_visualizer.app.bigquery.Client', MockBigQueryClient)

    response = client.get('/?selected_restaurant_name=Restaurant A')
    assert response.status_code == 200
    
    # Check if only Restaurant A's reviews are used for processing displayed on page
    # For example, the individual reviews section should only list Restaurant A
    # This is harder to check directly from response.data without parsing HTML extensively.
    # However, we can check if the `selected_restaurant_name` is passed correctly.
    assert b'<option value="Restaurant A" selected>Restaurant A</option>' in response.data
    
    # Test if pros/cons are filtered (example: 'Good portions' is not from Restaurant A)
    # The test for process_review_data directly is more robust for this.
    # Here, we're more focused on the route passing data correctly.
    # If we had access to the context passed to render_template, we could check that.
    # For now, we assume process_review_data is tested, and the route calls it correctly.
    assert b"Top 10 Review Pros" in response.data 
    assert b"great food" in response.data # From Restaurant A
    assert b"good portions" not in response.data # Not from Restaurant A in top pros when filtered

def test_filtering_logic_all_restaurants(client, monkeypatch):
    """Test the full route logic with 'All Restaurants' (no filter)."""
    class MockBigQueryClient:
        def query(self, query_string):
            class MockQueryJob:
                def result(self):
                    return SAMPLE_REVIEWS
            return MockQueryJob()

    monkeypatch.setattr('review_visualizer.app.bigquery.Client', MockBigQueryClient)

    response = client.get('/?selected_restaurant_name=') # Empty means all
    assert response.status_code == 200
    assert b'<option value="" selected>All Restaurants</option>' in response.data
    
    # Check if a pro from a restaurant other than A appears, indicating all data used
    assert b"good portions" in response.data # From Restaurant B, should be present

def test_process_review_data_specific_restaurant():
    """Test process_review_data when it receives data for only one restaurant."""
    filtered_reviews = [r for r in SAMPLE_REVIEWS if r['display_name'] == 'Restaurant A']
    top_pros, top_cons, avg_ratings, time_series = process_review_data(filtered_reviews)

    assert ('great food', 2) in top_pros
    assert ('friendly staff', 1) in top_pros
    assert len(top_pros) == 2

    assert ('a bit pricey', 2) in top_cons # Both from Restaurant A
    assert ('slow service', 1) in top_cons
    assert len(top_cons) == 2
    
    assert avg_ratings['Restaurant A'] == 4.0
    assert len(avg_ratings) == 1

    assert time_series['labels'] == ['2023-01', '2023-02']
    assert time_series['review_counts'] == [1, 1]
    assert time_series['average_ratings'] == [5.0, 3.0]

def test_app_config_for_bigquery_client_in_tests(client):
    """ Ensures that the BIGQUERY_CLIENT is None during tests if not mocked otherwise """
    # The client fixture already sets app.config['BIGQUERY_CLIENT'] = None
    # This test just verifies that the config is accessible and holds that value.
    assert app.config.get('BIGQUERY_CLIENT') is None
    
    # A more direct way to test if bigquery.Client() is called when not mocked:
    # with pytest.raises(SomeExpectedErrorIfClientIsNoneAndUsed):
    # client.get('/') # if the route tries to use a None client without a mock.
    # This is implicitly covered by tests that mock bigquery.Client, ensuring they *need* the mock.
    # The `test_index_route_loads` would fail if bigquery.Client() was called and was None or tried a real connection.
    
    # If we want to ensure no actual BQ call happens, we can try a get without a BQ mock
    # and expect it to fail gracefully or pass if the error handling in app.py is robust.
    # For now, the explicit mocking strategy in other tests is preferred.
    pass

# To run these tests:
# Ensure you are in the /app directory (parent of review_visualizer)
# Command: python -m pytest review_visualizer/tests/test_app.py
# Or: pytest review_visualizer/tests/test_app.py (if pytest is in PATH)
# Ensure __init__.py files are present in review_visualizer and review_visualizer/tests if needed for module discovery.
# For this structure, `python -m pytest` from `/app` should work.
# Adding an __init__.py to review_visualizer/tests might be good practice.
# And an __init__.py to review_visualizer to mark it as a package.

# Create __init__.py files for package structure
# touch review_visualizer/__init__.py
# touch review_visualizer/tests/__init__.py
# These are often needed for pytest to correctly discover modules, especially if running pytest from a different directory.
# The `run_in_bash_session` can be used for this if needed.
# For now, assuming `python -m pytest ...` handles paths correctly.

# Final check for the `test_index_route_loads` to ensure it handles the case where BQ might return empty.
def test_index_route_with_empty_bq_data(client, monkeypatch):
    class MockBigQueryClient:
        def query(self, query_string):
            class MockQueryJob:
                def result(self):
                    return [] # Empty list from BigQuery
            return MockQueryJob()

    monkeypatch.setattr('review_visualizer.app.bigquery.Client', MockBigQueryClient)
    
    response = client.get('/')
    assert response.status_code == 200
    assert b"<title>Review Visualizer</title>" in response.data
    assert b"No pros data available for the current selection." in response.data # Expecting no data messages
    assert b"No individual reviews to display for the current selection." in response.data
    assert b"All Restaurants" in response.data # Filter should still be there
    assert len(app.jinja_env.globals['restaurant_names']) == 0 # Check if restaurant_names is empty
    assert app.jinja_env.globals['top_pros'] == []
    assert app.jinja_env.globals['top_cons'] == []
    assert app.jinja_env.globals['average_restaurant_ratings'] == {}
    assert app.jinja_env.globals['reviews_over_time_chart_data'] == {}
