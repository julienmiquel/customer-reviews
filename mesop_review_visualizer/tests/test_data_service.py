import pytest
from mesop_review_visualizer.data_service import (
    process_review_data, 
    augment_reviews_with_ui_name, 
    get_bigquery_client, # For potential direct mocking if needed elsewhere
    fetch_raw_reviews, # To test it with mock BQ
    get_processed_review_data # To test the main service function
)
from datetime import datetime
from collections import Counter
from unittest.mock import patch # For mocking get_bigquery_client

# Sample data remains the same
COMPREHENSIVE_SAMPLE_REVIEWS = [
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 5, 'review_pros': ['Great food XA'], 'review_datetime': datetime(2023, 1, 10), 'latitude': 1.0, 'longitude': 1.0},
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Good service XA'], 'review_datetime': datetime(2023, 1, 11), 'latitude': 1.0, 'longitude': 1.0},
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 3, 'review_pros': ['Nice view YA'], 'review_datetime': datetime(2023, 2, 10), 'latitude': 2.0, 'longitude': 2.0},
    {'display_name': 'Restaurant B', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Tasty XB'], 'review_datetime': datetime(2023, 1, 12), 'latitude': 1.1, 'longitude': 1.1},
    {'display_name': 'Restaurant C', 'city': 'City Z', 'review_rating': 5, 'review_pros': ['Unique ZC'], 'review_datetime': datetime(2023, 3, 1), 'latitude': 3.0, 'longitude': 3.0},
    {'display_name': 'Restaurant D', 'city': None, 'review_rating': 2, 'review_pros': ['No city D'], 'review_datetime': datetime(2023, 3, 5), 'latitude': 4.0, 'longitude': 4.0},
    {'display_name': 'Restaurant E', 'city': '', 'review_rating': 3, 'review_pros': ['Empty city E'], 'review_datetime': datetime(2023, 3, 10), 'latitude': 5.0, 'longitude': 5.0},
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 2, 'review_pros': ['Okay YA'], 'review_datetime': datetime(2023, 2, 12), 'latitude': 2.0, 'longitude': 2.0},
    {'display_name': 'Restaurant F', 'city': 'City X', 'review_rating': 3, 'review_pros': ['New XF'], 'review_datetime': datetime(2023, 4, 1), 'latitude': 1.2, 'longitude': 1.2},
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

@pytest.fixture
def mock_bq_client(monkeypatch):
    """Fixture to mock the BigQuery client used by data_service functions."""
    def _get_mock_client(data_to_return):
        mock_client = MockBigQueryClient(data_to_return)
        monkeypatch.setattr("mesop_review_visualizer.data_service.get_bigquery_client", lambda: mock_client)
        return mock_client
    return _get_mock_client

def test_fetch_raw_reviews_with_mock_bq(mock_bq_client):
    """Test fetch_raw_reviews with a mocked BigQuery client."""
    expected_data = [{'id': 1, 'name': 'Review 1'}, {'id': 2, 'name': 'Review 2'}]
    mock_bq_client(expected_data) 
    result = fetch_raw_reviews()
    assert result == expected_data

def test_get_processed_review_data_with_mock_bq(mock_bq_client):
    """Test the main get_processed_review_data function."""
    mock_bq_client(COMPREHENSIVE_SAMPLE_REVIEWS)
    augmented_reviews = get_processed_review_data() 
    assert len(augmented_reviews) == len(COMPREHENSIVE_SAMPLE_REVIEWS)
    assert any(r['ui_display_name'] == 'Restaurant A (City X)' for r in augmented_reviews)
    assert any(r['ui_display_name'] == 'Restaurant B' for r in augmented_reviews)

def test_process_review_data_with_disambiguation():
    augmented_data = augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS)
    data_for_ra_city_x = [r for r in augmented_data if r['ui_display_name'] == 'Restaurant A (City X)']
    top_pros, top_cons, avg_ratings, time_series = process_review_data(data_for_ra_city_x)
    assert 'Restaurant A (City X)' in avg_ratings
    assert avg_ratings['Restaurant A (City X)'] == 4.5
    assert len(avg_ratings) == 1
    assert any(p[0] == 'great food xa' for p in top_pros)
    assert any(p[0] == 'good service xa' for p in top_pros)

    top_pros_all, _, avg_ratings_all, _ = process_review_data(augmented_data)
    assert 'Restaurant A (City X)' in avg_ratings_all
    assert avg_ratings_all['Restaurant A (City X)'] == 4.5
    assert 'Restaurant A (City Y)' in avg_ratings_all
    assert avg_ratings_all['Restaurant A (City Y)'] == 2.5
    assert 'Restaurant B' in avg_ratings_all
    assert avg_ratings_all['Restaurant B'] == 4.0

def test_augment_reviews_with_ui_name_logic():
    sample = [
        {'display_name': 'BK', 'city': 'Gotham', 'review_rating': 5, 'review_pros': [], 'review_datetime': datetime.now()},
        {'display_name': 'BK', 'city': 'Metropolis', 'review_rating': 4, 'review_pros': [], 'review_datetime': datetime.now()},
        {'display_name': 'Wayne Subs', 'city': 'Gotham', 'review_rating': 3, 'review_pros': [], 'review_datetime': datetime.now()},
        {'display_name': 'Daily Planet Cafe', 'city': None, 'review_rating': 2, 'review_pros': [], 'review_datetime': datetime.now()},
        {'display_name': 'Star Labs Coffee', 'city': '', 'review_rating': 1, 'review_pros': [], 'review_datetime': datetime.now()},
        {'display_name': 'Ace Chemicals', 'city': 'Gotham', 'review_rating': 5, 'review_pros': [], 'review_datetime': datetime.now()},
    ]
    augmented = augment_reviews_with_ui_name(sample) 
    ui_names = {r['ui_display_name'] for r in augmented}
    expected_ui_names = {
        'BK (Gotham)', 
        'BK (Metropolis)', 
        'Wayne Subs',
        'Daily Planet Cafe',
        'Star Labs Coffee',
        'Ace Chemicals'
    }
    assert ui_names == expected_ui_names
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'BK' and r['city'] == 'Gotham') == 'BK (Gotham)'
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'Wayne Subs') == 'Wayne Subs'
    assert next(r['ui_display_name'] for r in augmented if r['display_name'] == 'Daily Planet Cafe') == 'Daily Planet Cafe'

def test_data_service_functions_with_empty_input(mock_bq_client):
    mock_bq_client([]) 
    processed_data_empty_bq = get_processed_review_data()
    assert processed_data_empty_bq == []
    augmented_empty = augment_reviews_with_ui_name([])
    assert augmented_empty == []
    top_pros, top_cons, avg_ratings, time_series = process_review_data([])
    assert top_pros == []
    assert top_cons == []
    assert avg_ratings == {}
    assert time_series == {'labels': [], 'review_counts': [], 'average_ratings': []}
