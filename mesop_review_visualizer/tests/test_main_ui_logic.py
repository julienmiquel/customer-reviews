import pytest
from mesop_review_visualizer.main import (
    State,
    prepare_map_data,
    apply_filters_and_process_data,
    # For testing filter options, we might need to import SelectOption if comparing objects directly
    # or extract the logic for generating options into testable helper functions.
)
from mesop_review_visualizer.data_service import (
    augment_reviews_with_ui_name,
    # process_review_data is called by apply_filters_and_process_data, ensure it's tested in test_data_service
)
from datetime import datetime

# Copied from test_data_service.py for now. Ideally, this would be in a shared conftest.py or fixture.
COMPREHENSIVE_SAMPLE_REVIEWS_RAW = [
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 5, 'review_pros': ['Great food XA'], 'review_datetime': datetime(2023, 1, 10), 'latitude': 1.0, 'longitude': 1.0},
    {'display_name': 'Restaurant A', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Good service XA'], 'review_datetime': datetime(2023, 1, 11), 'latitude': 1.0, 'longitude': 1.0},
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 3, 'review_pros': ['Nice view YA'], 'review_datetime': datetime(2023, 2, 10), 'latitude': 2.0, 'longitude': 2.0},
    {'display_name': 'Restaurant B', 'city': 'City X', 'review_rating': 4, 'review_pros': ['Tasty XB'], 'review_datetime': datetime(2023, 1, 12), 'latitude': 1.1, 'longitude': 1.1},
    {'display_name': 'Restaurant C', 'city': 'City Z', 'review_rating': 5, 'review_pros': ['Unique ZC'], 'review_datetime': datetime(2023, 3, 1), 'latitude': 3.0, 'longitude': 3.0},
    {'display_name': 'Restaurant D', 'city': None, 'review_rating': 2, 'review_pros': ['No city D'], 'review_datetime': datetime(2023, 3, 5), 'latitude': 4.0, 'longitude': 4.0}, # Valid lat/lng
    {'display_name': 'Restaurant E', 'city': '', 'review_rating': 3, 'review_pros': ['Empty city E'], 'review_datetime': datetime(2023, 3, 10), 'latitude': None, 'longitude': 5.0}, # Invalid lat
    {'display_name': 'Restaurant A', 'city': 'City Y', 'review_rating': 2, 'review_pros': ['Okay YA'], 'review_datetime': datetime(2023, 2, 12), 'latitude': 2.0, 'longitude': 2.0},
    {'display_name': 'Restaurant F', 'city': 'City X', 'review_rating': 3, 'review_pros': ['New XF'], 'review_datetime': datetime(2023, 4, 1), 'latitude': 1.2, 'longitude': 1.2},
]

@pytest.fixture
def sample_augmented_reviews():
    # Use the actual augmentation function from data_service
    return augment_reviews_with_ui_name(COMPREHENSIVE_SAMPLE_REVIEWS_RAW)

# --- Tests for prepare_map_data ---
def test_prepare_map_data_structure_and_content(sample_augmented_reviews):
    map_data = prepare_map_data(sample_augmented_reviews)
    
    assert isinstance(map_data, list)
    
    # Expected restaurants on map (those with valid lat/lng)
    # Restaurant E has invalid lat, D has valid lat/lng
    expected_map_restaurants = {'Restaurant A', 'Restaurant B', 'Restaurant C', 'Restaurant D', 'Restaurant F'}
    map_restaurant_names = {item['name'] for item in map_data}
    assert map_restaurant_names == expected_map_restaurants
    
    for item in map_data:
        assert 'name' in item
        assert 'lat' in item
        assert 'lng' in item
        assert 'avg_rating' in item
        assert 'review_count' in item
        assert isinstance(item['lat'], float)
        assert isinstance(item['lng'], float)

def test_prepare_map_data_aggregation(sample_augmented_reviews):
    map_data = prepare_map_data(sample_augmented_reviews)
    
    res_a_info = next((item for item in map_data if item['name'] == 'Restaurant A'), None)
    assert res_a_info is not None
    # Restaurant A:
    # City X: 5, 4 (avg 4.5, count 2)
    # City Y: 3, 2 (avg 2.5, count 2)
    # Overall for map: (5+4+3+2) / 4 = 14 / 4 = 3.5
    assert res_a_info['avg_rating'] == 3.5
    assert res_a_info['review_count'] == 4
    # Lat/Lng for Restaurant A should be one of its locations (e.g., the first one encountered: 1.0, 1.0 or 2.0, 2.0)
    # The logic in prepare_map_data takes the first encountered lat/lng for a given original_name
    assert (res_a_info['lat'] == 1.0 and res_a_info['lng'] == 1.0) or \
           (res_a_info['lat'] == 2.0 and res_a_info['lng'] == 2.0)


    res_b_info = next((item for item in map_data if item['name'] == 'Restaurant B'), None)
    assert res_b_info is not None
    assert res_b_info['avg_rating'] == 4.0 # Single review
    assert res_b_info['review_count'] == 1
    assert res_b_info['lat'] == 1.1 and res_b_info['lng'] == 1.1

def test_prepare_map_data_empty_input():
    map_data = prepare_map_data([])
    assert map_data == []

def test_prepare_map_data_no_valid_lat_lng():
    reviews_no_lat_lng = [
        {'display_name': 'Restaurant X', 'city': 'City A', 'review_rating': 5, 'latitude': None, 'longitude': None},
        {'display_name': 'Restaurant Y', 'city': 'City B', 'review_rating': 4, 'latitude': 1.0, 'longitude': None},
    ]
    augmented = augment_reviews_with_ui_name(reviews_no_lat_lng)
    map_data = prepare_map_data(augmented)
    assert map_data == []


# --- Tests for apply_filters_and_process_data ---
# These will require a State object and potentially mocking data_service.process_review_data
# if we want to isolate the filtering logic of apply_filters_and_process_data itself.
# For now, we'll test its integrated behavior.

def test_apply_filters_no_filters(sample_augmented_reviews):
    test_state = State()
    test_state.all_augmented_reviews = sample_augmented_reviews
    test_state.data_loaded = True # Mark data as loaded

    apply_filters_and_process_data(state_instance=test_state)

    assert len(test_state.filtered_reviews_for_display) == len(sample_augmented_reviews)
    assert test_state.total_displayed_reviews == len(sample_augmented_reviews)
    # Check if processed data is populated (existence check, details are for test_data_service)
    assert test_state.top_pros is not None 
    assert test_state.average_restaurant_ratings_display is not None

def test_apply_filters_city_filter(sample_augmented_reviews):
    test_state = State()
    test_state.all_augmented_reviews = sample_augmented_reviews
    test_state.data_loaded = True
    test_state.selected_city_name = "City X"

    apply_filters_and_process_data(state_instance=test_state)
    
    expected_reviews_in_city_x = [
        r for r in sample_augmented_reviews if r.get('city') == "City X"
    ]
    assert len(test_state.filtered_reviews_for_display) == len(expected_reviews_in_city_x)
    assert test_state.total_displayed_reviews == len(expected_reviews_in_city_x)
    for review in test_state.filtered_reviews_for_display:
        assert review.get('city') == "City X"
    
    # Verify that average_restaurant_ratings_display only contains restaurants from City X
    # Expected ui_names in City X: 'Restaurant A (City X)', 'Restaurant B', 'Restaurant F'
    city_x_ui_names = {'Restaurant A (City X)', 'Restaurant B', 'Restaurant F'}
    assert set(test_state.average_restaurant_ratings_display.keys()) == city_x_ui_names


def test_apply_filters_restaurant_filter(sample_augmented_reviews):
    test_state = State()
    test_state.all_augmented_reviews = sample_augmented_reviews
    test_state.data_loaded = True
    selected_ui_name = "Restaurant A (City Y)" # This one is disambiguated
    test_state.selected_restaurant_ui_name = selected_ui_name

    apply_filters_and_process_data(state_instance=test_state)

    expected_reviews_for_ra_city_y = [
        r for r in sample_augmented_reviews if r.get('ui_display_name') == selected_ui_name
    ]
    assert len(test_state.filtered_reviews_for_display) == len(expected_reviews_for_ra_city_y)
    assert test_state.total_displayed_reviews == len(expected_reviews_for_ra_city_y)
    for review in test_state.filtered_reviews_for_display:
        assert review.get('ui_display_name') == selected_ui_name
    
    assert set(test_state.average_restaurant_ratings_display.keys()) == {selected_ui_name}

def test_apply_filters_city_and_restaurant_filter(sample_augmented_reviews):
    test_state = State()
    test_state.all_augmented_reviews = sample_augmented_reviews
    test_state.data_loaded = True
    test_state.selected_city_name = "City X"
    selected_ui_name_in_city_x = "Restaurant B" # This one is NOT disambiguated but unique in City X
    test_state.selected_restaurant_ui_name = selected_ui_name_in_city_x

    apply_filters_and_process_data(state_instance=test_state)

    expected_reviews = [
        r for r in sample_augmented_reviews 
        if r.get('city') == "City X" and r.get('ui_display_name') == selected_ui_name_in_city_x
    ]
    assert len(test_state.filtered_reviews_for_display) == len(expected_reviews)
    assert test_state.total_displayed_reviews == len(expected_reviews)
    for review in test_state.filtered_reviews_for_display:
        assert review.get('city') == "City X"
        assert review.get('ui_display_name') == selected_ui_name_in_city_x

    assert set(test_state.average_restaurant_ratings_display.keys()) == {selected_ui_name_in_city_x}

def test_apply_filters_with_empty_reviews(sample_augmented_reviews): # Test processing of empty list
    test_state = State()
    test_state.all_augmented_reviews = [] # Start with no reviews
    test_state.data_loaded = True # But data loading process itself was "done"

    apply_filters_and_process_data(state_instance=test_state)
    assert test_state.filtered_reviews_for_display == []
    assert test_state.total_displayed_reviews == 0
    assert test_state.top_pros == []
    assert test_state.top_cons == []
    assert test_state.average_restaurant_ratings_display == {}
    assert test_state.reviews_over_time_chart_data == {'labels': [], 'review_counts': [], 'average_ratings': []}

# Placeholder for filter option generation tests - might need refactor in main.py
# For now, this shows the intent. Actual implementation would depend on how options are generated.
# def test_city_filter_options_generation(sample_augmented_reviews):
#     # This logic is currently inline in main.page()
#     # To test properly, it might need to be extracted e.g.
#     # def get_city_options(all_reviews: list) -> list[me.SelectOption]: ...
#     # For now, let's simulate the expected output if such a function existed
#     expected_cities = sorted(list(set(r['city'] for r in sample_augmented_reviews if r.get('city'))))
#     # Actual test would call the (hypothetical) extracted function and check me.SelectOption list
#     assert "City X" in expected_cities

# def test_restaurant_filter_options_generation(sample_augmented_reviews):
#     # Similar to city options, this is inline.
#     # Test with no city selected
#     expected_restaurants_all = sorted(list(set(r['ui_display_name'] for r in sample_augmented_reviews if r.get('ui_display_name'))))
#     # Test with a city selected
#     city_selected = "City X"
#     filtered_by_city = [r for r in sample_augmented_reviews if r.get('city') == city_selected]
#     expected_restaurants_city_x = sorted(list(set(r['ui_display_name'] for r in filtered_by_city if r.get('ui_display_name'))))
#     pass # Placeholder for actual calls if logic is extracted
