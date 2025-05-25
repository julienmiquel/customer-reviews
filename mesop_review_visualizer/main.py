import mesop as me
# import mesop.labs as mel # Removed unused import
from . import data_service
import json 
from datetime import datetime # Added for review date formatting
from dataclasses import field # Import field for default_factory

# Charting Utility Functions
def create_bar_chart_spec(
    data_values: list, 
    x_field: str, 
    y_field: str, 
    x_title: str, 
    y_title: str, 
    chart_title: str,
    sort_order: str | None = "-y", # Use None for no sort or existing sort in data
    y_domain: list | None = None,
    color: str | None = None,
    width: str = "container",
    height: int = 300,
) -> dict:
    """Creates a Vega-Lite spec for a bar chart."""
    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": chart_title,
        "width": width,
        "height": height,
        "data": {"values": data_values},
        "mark": "bar",
        "encoding": {
            "x": {
                "field": x_field, 
                "type": "nominal", 
                "title": x_title,
                "axis": {"labelAngle": -45}, # Angle labels for better readability
            },
            "y": {
                "field": y_field, 
                "type": "quantitative", 
                "title": y_title,
            },
        },
    }
    if sort_order:
        spec["encoding"]["x"]["sort"] = sort_order
    if y_domain:
        spec["encoding"]["y"]["scale"] = {"domain": y_domain}
    if color:
        spec["encoding"]["color"] = {"value": color} # For fixed color bars
    return spec

def create_time_series_line_chart_spec(
    data_values: list, 
    x_field: str, 
    y_field: str, 
    color_field: str,
    x_title: str, 
    y_title_left: str, 
    y_title_right: str,
    chart_title: str,
    width: str = "container",
    height: int = 300,
) -> dict:
    """Creates a Vega-Lite spec for a multi-series line chart with dual Y-axes."""
    
    # Base layer for the first metric (e.g., review counts)
    layer1 = {
        "mark": {"type": "line", "point": True, "tooltip": True},
        "encoding": {
            "x": {"field": x_field, "type": "temporal", "timeUnit": "yearmonth", "title": x_title, "axis": {"format": "%b %Y"}},
            "y": {"field": y_field, "type": "quantitative", "title": y_title_left, "axis": {"grid": False}},
            "color": {"datum": "Number of Reviews", "type": "nominal"}, # Explicitly name the series
        },
         "transform": [{"filter": f"datum.{color_field} == 'Number of Reviews'"}]
    }
    
    # Base layer for the second metric (e.g., average ratings)
    layer2 = {
        "mark": {"type": "line", "point": True, "tooltip": True},
        "encoding": {
            "x": {"field": x_field, "type": "temporal", "timeUnit": "yearmonth", "title": x_title, "axis": {"format": "%b %Y"}},
            "y": {"field": y_field, "type": "quantitative", "title": y_title_right, "axis": {"grid": False}},
            "color": {"datum": "Average Rating", "type": "nominal"}, # Explicitly name the series
        },
        "transform": [{"filter": f"datum.{color_field} == 'Average Rating'"}]
    }

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": chart_title,
        "width": width,
        "height": height,
        "data": {"values": data_values},
        "layer": [layer1, layer2],
        "resolve": {"scale": {"y": "independent"}}, # Independent Y-axes
         "encoding": { # Common color encoding for legend
            "color": {"field": color_field, "type": "nominal", "title": "Metric"}
        }
    }
    return spec

@me.stateclass
class State:
    all_augmented_reviews: list = field(default_factory=list)
    filtered_reviews_for_display: list = field(default_factory=list)
    top_pros: list = field(default_factory=list)
    top_cons: list = field(default_factory=list)
    average_restaurant_ratings_display: dict = field(default_factory=dict)
    reviews_over_time_chart_data: dict = field(default_factory=dict)
    restaurants_map_data: list = field(default_factory=list)
    selected_restaurant_ui_name: str = ""
    selected_city_name: str = ""
    error_message: str | None = None
    data_loaded: bool = False
    # For summary display
    total_displayed_reviews: int = 0


def prepare_map_data(all_reviews_data_augmented: list) -> list:
    """
    Prepares map data from augmented reviews.
    Uses original 'display_name' for map keys and aggregates.
    """
    unique_restaurants_info_map = {} 
    for review in all_reviews_data_augmented:
        original_name = review.get('display_name') 
        lat = review.get('latitude')
        lng = review.get('longitude')
        if original_name and original_name not in unique_restaurants_info_map and lat is not None and lng is not None:
            try:
                unique_restaurants_info_map[original_name] = {'lat': float(lat), 'lng': float(lng)}
            except (ValueError, TypeError):
                print(f"Warning: Could not parse lat/lng for {original_name}: lat={lat}, lng={lng}")

    all_restaurant_aggregates_map = {} 
    for review in all_reviews_data_augmented: 
        original_name = review.get('display_name') 
        rating = review.get('review_rating')
        if original_name and rating is not None:
            if original_name not in all_restaurant_aggregates_map:
                all_restaurant_aggregates_map[original_name] = {'total_rating': 0.0, 'count': 0}
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
            all_average_ratings_map[name] = 0.0
            
    prepared_map_data = []
    for name, info in unique_restaurants_info_map.items():
        prepared_map_data.append({
            'name': name, 
            'lat': info['lat'],
            'lng': info['lng'],
            'avg_rating': all_average_ratings_map.get(name, 0.0),
            'review_count': all_restaurant_aggregates_map.get(name, {}).get('count', 0)
        })
    return prepared_map_data


def on_load_data(state_instance: State | None = None):
    state = state_instance if state_instance else me.state(State)
    if not state.data_loaded:
        try:
            state.all_augmented_reviews = data_service.get_processed_review_data()
            if state.all_augmented_reviews:
                state.restaurants_map_data = prepare_map_data(state.all_augmented_reviews)
            else: 
                state.restaurants_map_data = []
                state.error_message = "No review data was loaded from the data service."
                print("Warning: get_processed_review_data returned no data.")

            state.data_loaded = True
            apply_filters_and_process_data(state_instance=state) 
        except Exception as e:
            state.error_message = f"Error loading initial data: {str(e)}"
            print(f"Initial load error: {e}")
            state.data_loaded = True 


def apply_filters_and_process_data(state_instance: State | None = None):
    state = state_instance if state_instance else me.state(State)
    state.error_message = None # Clear previous errors before new processing

    # Ensure data_loaded is checked, especially if called directly in tests without on_load_data context
    if not state.all_augmented_reviews and not state.data_loaded: 
        # This might happen if called prematurely in a test or unexpected UI flow
        # If data_loaded is True but all_augmented_reviews is empty, that's a valid state (empty BQ result)
        print("apply_filters_and_process_data called when all_augmented_reviews is empty and data not marked as loaded.")
        # state.error_message = "Data not loaded before attempting to filter." # Optional: set error
        return


    temp_reviews = state.all_augmented_reviews
    if state.selected_city_name:
        temp_reviews = [r for r in temp_reviews if r.get('city') == state.selected_city_name]
    
    if state.selected_restaurant_ui_name:
        temp_reviews = [r for r in temp_reviews if r.get('ui_display_name') == state.selected_restaurant_ui_name]
    
    state.filtered_reviews_for_display = temp_reviews
    state.total_displayed_reviews = len(state.filtered_reviews_for_display)
    
    try:
        # Call process_review_data from data_service
        (state.top_pros, 
         state.top_cons, 
         state.average_restaurant_ratings_display, 
         state.reviews_over_time_chart_data) = data_service.process_review_data(state.filtered_reviews_for_display)
    except Exception as e:
        state.error_message = f"Error processing review data: {str(e)}"
        print(f"Error in apply_filters_and_process_data calling process_review_data: {e}")
        # Reset processed data to avoid displaying stale info
        state.top_pros = []
        state.top_cons = []
        state.average_restaurant_ratings_display = {}
        state.reviews_over_time_chart_data = {}


def on_city_filter_change(event: me.SelectSelectionChangeEvent, state_instance: State | None = None):
    state = state_instance if state_instance else me.state(State)
    state.selected_city_name = event.value
    state.selected_restaurant_ui_name = "" 
    apply_filters_and_process_data(state_instance=state)

def on_restaurant_filter_change(event: me.SelectSelectionChangeEvent, state_instance: State | None = None):
    state = state_instance if state_instance else me.state(State)
    state.selected_restaurant_ui_name = event.value
    apply_filters_and_process_data(state_instance=state)

@me.page(
    path="/",
    title="Review Visualizer"
)
def page():
    state = me.state(State)
    # Link CSS. Consider moving to me.head() if Mesop evolves to support it better.
    me.Html("""<link rel="stylesheet" href="/static/style.css">""")

    # Trigger data load if not already done. Guarded internally by state.data_loaded.
    on_load_data(state_instance=state) 

    # Main page container - attempting to mimic body styles
    with me.box(style=me.Style(
        padding=me.Padding.all(20), 
        background="#f4f4f4", # Original body background-color
        color="#333", # Original body color
        font_family="sans-serif" # Original body font-family
        )
    ): 
        me.text(
            "Review Visualizer - Mesop Edition", 
            type="headline-4", # Using a larger headline type
            style=me.Style(
                color="#2c3e50", # Original h1, h2 color
                text_align="center",
                margin=me.Margin(bottom=25) 
            )
        )

        if state.error_message:
            # Styled error message box, mimicking .error class
            with me.box(style=me.Style(
                background="#e74c3c", 
                color="white", 
                padding=me.Padding.all(15),
                margin=me.Margin(bottom=20),
                border_radius=5,
                text_align="center"
            )):
                me.text(state.error_message, style=me.Style(font_weight="bold"))

        # --- Filter UI ---
        # Mimicking .filter-container
        with me.box(style=me.Style(
            display="flex", 
            flex_direction="row", 
            gap="20px", 
            margin=me.Margin(bottom=25), # Original margin-bottom
            padding=me.Padding.all(15), # Original padding
            background="#fff", # Original background-color
            border_radius=8, # Original border-radius
            # box_shadow="0 2px 4px rgba(0,0,0,0.1)", # This would be ideally handled by a class or global style
            justify_content="center", 
            align_items="center",
            flex_wrap="wrap" # Allow filters to wrap on smaller screens
            )
        ):
            # City Filter
            city_names = [""] + sorted(list(set(r['city'] for r in state.all_augmented_reviews if r.get('city'))))
            city_options = [me.SelectOption(label="All Cities" if not city else city, value=city) for city in city_names]
            me.Select(
                label="Filter by City",
                options=city_options,
                value=state.selected_city_name,
                # Pass state to handler for testing, Mesop will call it with just event in prod
                on_selection_change=lambda e: on_city_filter_change(e, state_instance=state),
                style=me.Style(width="250px", margin=me.Margin.symmetric(horizontal=5))
            )

            # Restaurant Filter
            restaurant_source_list = state.all_augmented_reviews
            if state.selected_city_name: 
                restaurant_source_list = [
                    r for r in state.all_augmented_reviews if r.get('city') == state.selected_city_name
                ]
            
            restaurant_ui_names = [""] + sorted(list(set(
                r['ui_display_name'] for r in restaurant_source_list if r.get('ui_display_name')
            )))
            restaurant_options = [me.SelectOption(label="All Restaurants" if not name else name, value=name) for name in restaurant_ui_names]
            me.Select(
                label="Filter by Restaurant",
                options=restaurant_options,
                value=state.selected_restaurant_ui_name,
                on_selection_change=lambda e: on_restaurant_filter_change(e, state_instance=state),
                style=me.Style(width="350px", margin=me.Margin.symmetric(horizontal=5)) 
            )
        
        # Common style for data section boxes, mimicking .data-section
        data_section_style = me.Style(
            background="#fff", 
            padding=me.Padding.all(20), # Original padding
            border_radius=8, 
            # box_shadow="0 2px 4px rgba(0,0,0,0.1)", # Relies on global CSS if me.box can pick up .data-section class or similar
            margin=me.Margin(bottom=20) 
        )
        # Common style for chart titles, mimicking .data-section h3
        chart_title_style = me.Style(
            text_align="center", 
            margin=me.Margin(top=0, bottom=10), # Original margin-top: 0
            color="#3498db" # Original color
        )
        # Common style for "no data" messages, mimicking .no-data
        no_data_style = me.Style(
            text_align="center", 
            color="#7f8c8d", # Original color
            padding=me.Padding.all(20) # Original padding
        )

        # --- Charts Section ---
        if state.data_loaded and not state.error_message:
            # Map Section (before charts)
            # Original: <div class="data-section full-width-section" id="map-container-wrapper">
            with me.box(style=data_section_style): # full-width is default for a single box in column
                me.text("Restaurant Performance Map", type="subtitle-1", style=chart_title_style)
                google_map_component(state.restaurants_map_data)

            # Row 1: Pros and Cons Charts (mimicking .container for charts)
            with me.box(style=me.Style(display="flex", flex_direction="row", gap="20px", margin=me.Margin(bottom=20))):
                # Mimicking .data-section for each chart
                with me.box(style=data_section_style.update(flex_grow=1, flex_basis="0", border=me.Border.all(color="#eee"))): # Added light border if shadow doesn't work
                    me.text("Top 10 Review Pros", type="subtitle-1", style=chart_title_style)
                    pros_chart_component(state.top_pros, no_data_style)
                with me.box(style=data_section_style.update(flex_grow=1, flex_basis="0", border=me.Border.all(color="#eee"))):
                    me.text("Top 10 Review Cons", type="subtitle-1", style=chart_title_style)
                    cons_chart_component(state.top_cons, no_data_style)
            
            # Row 2: Average Restaurant Ratings Chart
            with me.box(style=data_section_style.update(border=me.Border.all(color="#eee"))): # .data-section.full-width-section
                 me.text("Average Restaurant Ratings", type="subtitle-1", style=chart_title_style)
                 avg_ratings_chart_component(state.average_restaurant_ratings_display, no_data_style)

            # Row 3: Review Trends Over Time Chart
            with me.box(style=data_section_style.update(border=me.Border.all(color="#eee"))): # .data-section.full-width-section
                 me.text("Review Trends Over Time", type="subtitle-1", style=chart_title_style)
                 time_series_chart_component(state.reviews_over_time_chart_data, no_data_style)
            
            # Individual Reviews Section (after charts)
            individual_reviews_component(state.filtered_reviews_for_display, no_data_style)

        elif not state.error_message and not state.data_loaded:
             me.text("Loading data or no data available based on current filters...", style=no_data_style)
        elif not state.error_message and state.data_loaded and not state.all_augmented_reviews:
             me.text("No review data found from the source.", style=no_data_style)

# Individual Reviews Component
def individual_reviews_component(reviews_list: list, no_data_style: me.Style):
    state = me.state(State) 
    if not reviews_list:
        me.text("No individual reviews to display for the current selection.", style=no_data_style)
        return

    # Main container for all reviews (simulating .review-section)
    with me.box(style=me.Style(width="100%", margin_top=30)): # Original: margin-top: 30px;
        me.text(
            "Individual Reviews (Sample for " + (state.selected_restaurant_ui_name or state.selected_city_name or "All Locations") + ")", 
            type="headline-6", # Approximating h2
            style=me.Style(
                text_align="center", 
                margin=me.Margin(bottom=20),
                color="#2c3e50" # Original h2 color
            )
        )
        
        # Style for each review card, mimicking .review
        review_box_style = me.Style(
            background="#fff", 
            border=me.Border.all(width=1, color="#ccc"), # Original border
            margin=me.Margin(bottom=20), # Original margin-bottom
            padding=me.Padding.all(15), # Original padding
            border_radius=8, # Original border-radius
            # box_shadow="0 2px 4px rgba(0,0,0,0.05)" # Relies on global CSS
        )
        # Style for review title, mimicking .review h4
        review_title_style = me.Style(font_weight="bold", color="#16a085", margin=me.Margin(top=0, bottom=5)) 
        # Style for review meta, mimicking .review-meta
        review_meta_style = me.Style(font_size="0.9em", color="#555", margin=me.Margin(bottom=10)) 
        # Style for review text paragraph
        review_paragraph_style = me.Style(margin=me.Margin(bottom=5))
        # Style for Pros/Cons titles
        pros_cons_title_style = me.Style(font_weight="bold", margin=me.Margin(top=10, bottom=5)) # Original .pros, .cons margin-top
        # Style for Pros/Cons list container
        pros_cons_list_style = me.Style(padding=me.Padding(left=20), margin=me.Margin.symmetric(vertical=5)) # Original ul padding & margin
        # Style for Pros items, mimicking .pros li
        pros_item_style = me.Style(color="#27ae60", padding=me.Padding.all(2)) 
        # Style for Cons items, mimicking .cons li
        cons_item_style = me.Style(color="#c0392b", padding=me.Padding.all(2)) 

        for review_data in reviews_list[:5]: # Display up to 5 reviews
            with me.box(style=review_box_style):
                # Review Title (ui_display_name)
                me.text(
                    review_data.get('ui_display_name', review_data.get('display_name', 'N/A')), 
                    style=review_title_style
                )

                # Rating and Date
                review_dt_obj = review_data.get('review_datetime')
                dt_str = "N/A"
                if isinstance(review_dt_obj, datetime):
                    dt_str = review_dt_obj.strftime('%Y-%m-%d - %H:%M:%S')
                elif isinstance(review_dt_obj, str): 
                    try:
                        # Attempt to parse if it's a string, common in some data sources
                        parsed_dt = datetime.fromisoformat(review_dt_obj.replace('Z', '+00:00'))
                        dt_str = parsed_dt.strftime('%Y-%m-%d - %H:%M:%S')
                    except ValueError:
                        dt_str = review_dt_obj # Use as-is if not parsable
                
                me.text(
                    f"Rating: {review_data.get('review_rating', 'N/A')} | Date: {dt_str}",
                    style=review_meta_style
                )

                # Review Text
                if review_data.get('review_text'):
                    me.text(review_data.get('review_text'), style=review_paragraph_style)

                # Review Pros
                review_pros = review_data.get('review_pros')
                if review_pros:
                    me.text("Pros:", style=pros_cons_title_style) 
                    with me.box(style=pros_cons_list_style): 
                        if isinstance(review_pros, str):
                            me.text(f"• {review_pros}", style=pros_item_style)
                        elif isinstance(review_pros, list):
                            for pro_item in review_pros:
                                if pro_item and isinstance(pro_item, str): # Check pro_item is not None/empty and is string
                                     me.text(f"• {pro_item}", style=pros_item_style)
                        else: 
                            me.text(f"• {str(review_pros)}", style=pros_item_style)


                # Review Cons
                review_cons = review_data.get('review_cons')
                if review_cons:
                    me.text("Cons:", style=pros_cons_title_style) 
                    with me.box(style=pros_cons_list_style): 
                        if isinstance(review_cons, str):
                            me.text(f"• {review_cons}", style=cons_item_style)
                        elif isinstance(review_cons, list):
                            for con_item in review_cons:
                                 if con_item and isinstance(con_item, str): # Check con_item is not None/empty and is string
                                    me.text(f"• {con_item}", style=cons_item_style)
                        else: 
                             me.text(f"• {str(review_cons)}", style=cons_item_style)


# Google Map Component
def google_map_component(map_data_list: list):
    no_data_style = me.Style(text_align="center", color="#7f8c8d", padding=me.Padding.all(20))
    if not map_data_list:
        return me.text("No restaurant location data available to display on map.", style=no_data_style)

    json_map_data = json.dumps(map_data_list)

    # The API key is hardcoded as in the original HTML.
    # TODO: Move API key to environment variable or configuration
    gmaps_api_key = "AIzaSyCEhbTSzY2JF818fyKx6I4VpzMuAOav_8w" # From original HTML

    # initMap function extracted from the original index.html
    # Ensure all JS logic is correctly transferred.
    # Note: Using an f-string for a large JS block. Careful with braces {}
    # if they are intended for JS and not Python's f-string interpolation.
    # In this specific initMap, there are no such JS braces that conflict.
    init_map_js = f"""
        const restaurantsMapData = {json_map_data};

        async function initMap() {{
            const mapElement = document.getElementById("map");

            if (!mapElement) {{
                console.error("Map element not found!");
                return;
            }}
            if (!google || !google.maps || !google.maps.importLibrary) {{
                console.error("Google Maps API or importLibrary not loaded!");
                mapElement.innerHTML = '<p class="no-data" style="text-align:center; padding: 20px; color: #7f8c8d;">Error: Google Maps API failed to load.</p>';
                return;
            }}
            
            const {{ AdvancedMarkerElement, PinElement }} = await google.maps.importLibrary("marker");

            if (!restaurantsMapData || restaurantsMapData.length === 0) {{
                mapElement.innerHTML = '<p class="no-data" style="text-align:center; padding: 20px; color: #7f8c8d;">No restaurant location data available to display on map.</p>';
                console.log("No restaurant data for map.");
                return;
            }}

            let mapCenter = {{ lat: 40.7128, lng: -74.0060 }}; 
            let initialZoom = 4; 

            let totalLat = 0;
            let totalLng = 0;
            let validCoordsCount = 0;
            restaurantsMapData.forEach(r => {{
                if (typeof r.lat === 'number' && typeof r.lng === 'number') {{
                    totalLat += r.lat;
                    totalLng += r.lng;
                    validCoordsCount++;
                }}
            }});

            if (validCoordsCount > 0) {{
                mapCenter = {{ lat: totalLat / validCoordsCount, lng: totalLng / validCoordsCount }};
                initialZoom = 6; 
            }}
            if (validCoordsCount === 1 && restaurantsMapData.length === 1 ) {{ 
                mapCenter = {{ lat: restaurantsMapData[0].lat, lng: restaurantsMapData[0].lng }};
                initialZoom = 12;
            }}

            const map = new google.maps.Map(mapElement, {{
                zoom: initialZoom,
                center: mapCenter,
                mapId: 'DEMO_MAP_ID' // Required for AdvancedMarkerElement
            }});

            const infoWindow = new google.maps.InfoWindow();
            const markers = [];

            restaurantsMapData.forEach(restaurant => {{
                if (typeof restaurant.lat !== 'number' || typeof restaurant.lng !== 'number') {{
                    console.warn(`Skipping restaurant with invalid lat/lng: ${{restaurant.name}}`);
                    return; 
                }}

                let pinBackgroundColor;
                if (restaurant.avg_rating >= 4.0) {{
                    pinBackgroundColor = 'green';
                }} else if (restaurant.avg_rating >= 3.0) {{
                    pinBackgroundColor = 'orange'; 
                }} else {{
                    pinBackgroundColor = 'red';
                }}
                const pinElement = new PinElement({{
                    background: pinBackgroundColor,
                    borderColor: 'grey', 
                    glyphColor: 'white',
                }});

                const marker = new AdvancedMarkerElement({{
                    map: map,
                    position: {{ lat: restaurant.lat, lng: restaurant.lng }},
                    title: restaurant.name,
                    content: pinElement.element,
                    gmpClickable: true,
                }});

                marker.addListener('click', () => {{
                    const content = `
                        <div class="info-window-content">
                            <h4>${{restaurant.name}}</h4>
                            <p><strong>Avg. Rating:</strong> ${{restaurant.avg_rating ? restaurant.avg_rating.toFixed(1) : 'N/A'}} ★</p>
                            <p><strong>Total Reviews:</strong> ${{restaurant.review_count !== undefined ? restaurant.review_count : 'N/A'}}</p>
                        </div>
                    `;
                    infoWindow.setContent(content);
                    infoWindow.open(map, marker);
                }});
                markers.push(marker);
            }});
        }}
    """

    # The div id="map" will be styled by the global style.css
    # which includes #map { height: 500px; width: 100%; background-color: #e0e0e0; }
    map_html_structure = f"""
        <div id="map"></div>
        <script type="text/javascript">
            {init_map_js}
        </script>
        <script async defer src="https://maps.googleapis.com/maps/api/js?key={gmaps_api_key}&callback=initMap&libraries=marker,places"></script>
    """
    # Important: The Google Maps API script must be loaded for initMap to be called.
    # The `async defer` and `callback=initMap` handles this.
    # The script containing initMap definition and data should be available when the callback is executed.
    
    return me.unsafe_html(map_html_structure)


# Chart Component Functions
def pros_chart_component(top_pros_data: list, no_data_style: me.Style):
    if not top_pros_data:
        me.text("No pros data available for the current selection.", style=no_data_style)
        return
    
    transformed_data = [{"pro": item[0], "count": item[1]} for item in top_pros_data]
    spec = create_bar_chart_spec(
        data_values=transformed_data,
        x_field="pro",
        y_field="count",
        x_title="Pros",
        y_title="Count",
        chart_title="", # Title provided by text component above
        color="#4CAF50" # Greenish color for pros
    )
    me.Plotלינק(spec=spec)

def cons_chart_component(top_cons_data: list, no_data_style: me.Style):
    if not top_cons_data:
        me.text("No cons data available for the current selection.", style=no_data_style)
        return
        
    transformed_data = [{"con": item[0], "count": item[1]} for item in top_cons_data]
    spec = create_bar_chart_spec(
        data_values=transformed_data,
        x_field="con",
        y_field="count",
        x_title="Cons",
        y_title="Count",
        chart_title="",
        color="#F44336" # Reddish color for cons
    )
    me.Plotלינק(spec=spec)

def avg_ratings_chart_component(avg_ratings_data: dict, no_data_style: me.Style):
    if not avg_ratings_data:
        me.text("No rating data available for the current selection.", style=no_data_style)
        return
        
    # Sort by rating descending, then by name for tie-breaking
    sorted_ratings = sorted(avg_ratings_data.items(), key=lambda item: (item[1], item[0]), reverse=True)
    transformed_data = [{"restaurant": item[0], "rating": item[1]} for item in sorted_ratings]
    
    spec = create_bar_chart_spec(
        data_values=transformed_data,
        x_field="restaurant",
        y_field="rating",
        x_title="Restaurant",
        y_title="Average Rating (out of 5)",
        chart_title="",
        y_domain=[0, 5],
        color="#2196F3", # Blueish color for ratings
        sort_order=None # Use data's sort order
    )
    me.Plotלינק(spec=spec)

def time_series_chart_component(time_series_data: dict, no_data_style: me.Style):
    if not time_series_data or not time_series_data.get('labels') or not time_series_data.get('review_counts'):
        me.text("No time series data available for the current selection.", style=no_data_style)
        return

    labels = time_series_data['labels']
    review_counts = time_series_data['review_counts']
    average_ratings = time_series_data.get('average_ratings', []) # Might be missing if all counts are 0

    transformed_data = []
    for i, label in enumerate(labels):
        transformed_data.append({"month": label, "value": review_counts[i], "metric": "Number of Reviews"})
        if i < len(average_ratings): # Ensure average_ratings has corresponding entry
             transformed_data.append({"month": label, "value": average_ratings[i], "metric": "Average Rating"})
    
    spec = create_time_series_line_chart_spec(
        data_values=transformed_data,
        x_field="month",
        y_field="value",
        color_field="metric",
        x_title="Month",
        y_title_left="Number of Reviews",
        y_title_right="Average Rating (out of 5)",
        chart_title=""
    )
    me.Plotלינק(spec=spec)

# Optional: Basic app configuration for labs if needed later
# me.colab_run(default_port=8080) # Or similar for local dev if preferred by Mesop setup
