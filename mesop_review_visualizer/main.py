import mesop as me
import json
from datetime import datetime
from .data_service import fetch_processed_data, process_review_data
from typing import List, Dict, Any, Optional 

# Module-level constants and shared utilities

# TODO: Hardcoded API Key - should be moved to environment variable or secure config for production.
GMAPS_API_KEY = "AIzaSyCEhbTSzY2JF818fyKx6I4VpzMuAOav_8w" 

# Common style for displaying "no data" messages in UI components.
NO_DATA_STYLE = me.Style(
    text_align="center", 
    color="#7f8c8d", 
    padding=me.Padding.all(20), 
    font_style="italic", 
    display="flex", 
    align_items="center",
    justify_content="center",
    height="100%" # Ensures text is centered in fixed-height containers like chart boxes.
)

# Standard style for main content sections.
section_style = me.Style(
    background="#fff", 
    padding=me.Padding.all(20), 
    border_radius=8, 
    margin=me.Margin(bottom=20), 
    box_shadow="0 2px 4px rgba(0,0,0,0.1)"
)

# Standard style for containers holding charts or their "no data" messages.
# Ensures consistent height for chart areas.
chart_container_style = me.Style(
    height=350, 
    display="flex", 
    align_items="center", 
    justify_content="center"
)

# Standard style for map placeholder elements, used when map data is unavailable.
map_placeholder_style = me.Style( 
    height=500, 
    background="#e0e0e0", 
    border=me.Border.all(me.BorderSide(style="dashed", color="#ccc")), 
    display="flex", 
    align_items="center", 
    justify_content="center"
)

def create_chart_html(canvas_id: str, chart_type: str, chart_data_json: dict, chart_options_json: dict) -> str:
    """
    Generates the HTML and JavaScript for a Chart.js chart.
    Includes logic to destroy and re-create chart instances to prevent rendering issues on updates.

    Args:
        canvas_id: The HTML ID for the canvas element.
        chart_type: The type of chart (e.g., 'bar', 'line').
        chart_data_json: The data object for the chart, already JSON-serialized.
        chart_options_json: The options object for the chart, already JSON-serialized.

    Returns:
        A string containing the HTML and JavaScript for the chart.
    """
    # js_data and js_options are already JSON strings if coming from render_chart_section
    # If called directly, ensure they are serialized json.dumps(chart_data)
    # For this refactor, chart_data_for_js and chart_options_for_js will be dicts,
    # so json.dumps is needed here.
    js_data = json.dumps(chart_data_json)
    js_options = json.dumps(chart_options_json)

    return f"""
    <div style="width: 100%; height: 350px;"> 
        <canvas id="{canvas_id}"></canvas>
    </div>
    <script>
        if (typeof Chart !== 'undefined') {{
            const ctx_{canvas_id} = document.getElementById('{canvas_id}');
            if (ctx_{canvas_id}) {{
                if (window.chartInstances && window.chartInstances.{canvas_id}) {{
                    window.chartInstances.{canvas_id}.destroy();
                }}
                if (!window.chartInstances) {{
                    window.chartInstances = {{}};
                }}
                window.chartInstances.{canvas_id} = new Chart(ctx_{canvas_id}, {{
                    type: '{chart_type}',
                    data: {js_data},
                    options: {js_options}
                }});
            }}
        }}
    </script>
    """

def get_map_embed_html(
    map_element_id: str,
    map_data_js_variable_name: str,
    map_data_json_str: str,
    map_init_function_name: str,
    google_map_id_string: str, # For differentiating map styles/features if needed
    gmaps_api_key: str 
) -> str:
    """
    Generates the HTML and JavaScript to embed a Google Map.
    Handles dynamic loading of the Google Maps API script and map initialization.

    Args:
        map_element_id: HTML ID for the div where the map will be rendered.
        map_data_js_variable_name: Name for the JavaScript variable holding map data.
        map_data_json_str: JSON string of map data.
        map_init_function_name: Unique name for the map initialization JavaScript function.
        google_map_id_string: The Map ID for styling the specific Google Map instance.
        gmaps_api_key: The Google Maps API key.

    Returns:
        A string containing HTML and JavaScript for the Google Map.
    """
    api_loaded_flag = f"window.googleMapApiLoaded_{map_element_id}" 

    return f'''
    <div id="{map_element_id}" style="height: 500px; width: 100%; background-color: #e0e0e0;">Loading map...</div>
    <script>
        const {map_data_js_variable_name} = {map_data_json_str}; 
        
        async function {map_init_function_name}() {{{{ 
            const mapElement = document.getElementById("{map_element_id}");
            if (!mapElement) {{{{
                console.error("Map element '{map_element_id}' not found"); return;
            }}}}
            if (typeof google === 'undefined' || !google.maps || !google.maps.importLibrary) {{{{
                mapElement.innerHTML = '<p style="text-align:center;padding:20px;">Error: Google Maps API not loaded.</p>';
                return;
            }}}}
            try {{{{ 
                const {{{{ AdvancedMarkerElement, PinElement }}}} = await google.maps.importLibrary("marker"); 
                const mapData = {map_data_js_variable_name}; 

                if (!mapData || mapData.length === 0) {{{{
                    mapElement.innerHTML = '<p style="text-align:center;padding:20px; font-style:italic; color:#555;">No restaurant location data for map.</p>';
                    return;
                }}}}
                
                let mapCenter = {{{{ lat: 40.7128, lng: -74.0060 }}}}; let initialZoom = 2; 
                let totalLat = 0; let totalLng = 0; let validCoordsCount = 0;
                mapData.forEach(r => {{{{ 
                    if (typeof r.lat === 'number' && typeof r.lng === 'number') {{{{
                        totalLat += r.lat; totalLng += r.lng; validCoordsCount++;
                    }}}}
                }}}});
                if (validCoordsCount > 0) {{{{ 
                    mapCenter = {{{{ lat: totalLat / validCoordsCount, lng: totalLng / validCoordsCount }}}}; initialZoom = 5; 
                }}}}
                if (validCoordsCount === 1 && mapData.length === 1 ) {{{{ 
                    mapCenter = {{{{ lat: mapData[0].lat, lng: mapData[0].lng }}}}; initialZoom = 12;
                }}}}

                const map = new google.maps.Map(mapElement, {{{{ 
                    zoom: initialZoom,
                    center: mapCenter,
                    mapId: '{google_map_id_string}'
                }}}});
                const infoWindow = new google.maps.InfoWindow();

                mapData.forEach(restaurant => {{{{ 
                    if (typeof restaurant.lat !== 'number' || typeof restaurant.lng !== 'number') return;
                    let pinColorVal = restaurant.avg_rating >= 4.0 ? 'green' : (restaurant.avg_rating >= 3.0 ? 'orange' : 'red');
                    const pin = new PinElement({{{{ background: pinColorVal, borderColor: 'grey', glyphColor: 'white' }}}}); 
                    const marker = new AdvancedMarkerElement({{{{ 
                        map: map,
                        position: {{{{ lat: restaurant.lat, lng: restaurant.lng }}}}, 
                        title: restaurant.name,
                        content: pin.element,
                        gmpClickable: true
                    }}}});
                    marker.addListener('click', () => {{{{ 
                        infoWindow.setContent(
                            '<div style="padding:10px"><h4>' + restaurant.name + '</h4>' +
                            '<p style="margin:0;font-size:0.9em;">Avg: ' + 
                            (restaurant.avg_rating ? restaurant.avg_rating.toFixed(1) : 'N/A') + 
                            ' ★ | Reviews: ' + restaurant.review_count + '</p></div>'
                        );
                        infoWindow.open(map, marker);
                    }}}});
                }}}});
            }}}} catch (e) {{{{ 
                console.error("Error initializing map '{map_element_id}':", e);
                mapElement.innerHTML = '<p style="text-align:center;padding:20px;">Error displaying map.</p>';
            }}}} 
        }}}} 

        if (typeof {api_loaded_flag} === 'undefined') {{{{
            {api_loaded_flag} = true; 
            const script = document.createElement('script');
            script.src = `https://maps.googleapis.com/maps/api/js?key=${gmaps_api_key}&callback={map_init_function_name}&libraries=marker,places&loading=async&defer=true`;
            script.onerror = () => {{{{
                document.getElementById("{map_element_id}").innerHTML = '<p style="text-align:center;padding:20px; font-style:italic; color:#555;">Error: Failed to load Google Maps script.</p>';
            }}}};
            document.head.appendChild(script);
        }}}} else if (typeof google !== 'undefined' && google.maps && typeof window['{map_init_function_name}'] === 'function') {{{{
            window['{map_init_function_name}']();
        }}}} else if (typeof google !== 'undefined' && google.maps && typeof {map_init_function_name} === 'function') {{{{
             {map_init_function_name}();
        }}}}
    </script>
    '''

# New Chart Section Rendering Function
def render_chart_section(
    title_text: str,
    chart_canvas_id: str,
    chart_type: str,
    chart_data_for_js: Optional[dict],    # Data for create_chart_html's 'data' param
    chart_options_for_js: Optional[dict], # Data for create_chart_html's 'options' param
    data_exists: bool,          # Boolean to indicate if data is present
    custom_section_style: Optional[me.Style] = None # For additional styling like flex_basis
):
    """
    Renders a complete chart section including title, chart, and no-data fallback.
    Uses module-level style constants and the create_chart_html function.
    """
    current_section_style = section_style # Start with base section_style
    if custom_section_style:
        current_section_style = current_section_style.extend(custom_section_style)

    with me.box(style=current_section_style):
        me.text(title_text, type="headline-3", style=me.Style(text_align="center", margin=me.Margin(bottom=15)))
        if data_exists:
            # Ensure chart_data_for_js and chart_options_for_js are not None before passing
            if chart_data_for_js is not None and chart_options_for_js is not None:
                 me.embed_html(create_chart_html(chart_canvas_id, chart_type, chart_data_for_js, chart_options_for_js))
            else:
                # This case should ideally not be hit if data_exists is true, but as a fallback:
                with me.box(style=chart_container_style): 
                     me.text("Chart data is missing despite data_exists flag.", style=NO_DATA_STYLE) 
        else:
            with me.box(style=chart_container_style): 
                 # Simplified no-data message using parts of the title
                 simple_title_part = title_text.lower().replace('overall','').replace('top 10','').replace('(filtered)','').strip()
                 me.text(f"No data available for {simple_title_part}.", style=NO_DATA_STYLE) 


# Navigation Component Function
def render_nav():
    """Renders the main navigation bar for the application."""
    with me.box(style=me.Style(
        background_color="#333",  
        padding=me.Padding.symmetric(vertical=10, horizontal=20),
        margin=me.Margin(bottom=20), 
        display="flex",
        justify_content="center", 
        gap="20px" 
    )):
        me.link(text="General Overview", url="/general", style=me.Style(color="white", text_decoration="none", font_size="1.1em"))
        me.link(text="Detailed Filters", url="/detailed", style=me.Style(color="white", text_decoration="none", font_size="1.1em"))

# Root Page: Redirects to the General Overview page by default.
@me.page(path="/", title="Restaurant Review Dashboard")
def root_page():
    """Handles the root path by redirecting to the general overview page."""
    me.navigate("/general") 


# General Information Page: Displays overall statistics and charts.
@me.page(path="/general", title="Restaurant Review Dashboard - General Overview")
def general_page():
    """Renders the general overview page with summary statistics and overall charts."""
    me.style(r"""
    body {{
        font-family: sans-serif;
        margin: 0; 
        padding: 0; 
        background-color: #f4f4f4;
        color: #333;
    }}
    """)

    me.html(r"""
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment@2.29.1/moment.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@1.0.0/dist/chartjs-adapter-moment.min.js"></script>
    """)

    render_nav() 

    with me.box(style=me.Style(max_width="1200px", margin=me.Margin.symmetric(horizontal="auto"), padding=me.Padding.all(20))):
        me.text("Restaurant Review Dashboard - General Overview", type="headline-1", style=me.Style(text_align="center", margin=me.Margin(bottom=30)))

        all_reviews_data, city_names, restaurants_map_data, error_message = fetch_processed_data()

        if error_message:
            with me.box(style=me.Style(background="#ffdddd", border=me.Border.all(me.BorderSide(color="red", width=1)), padding=me.Padding.all(15), margin=me.Margin(bottom=20), border_radius=5, text_align="center")):
                me.text("Data Loading Error:", weight="bold", style=me.Style(color="red"))
                me.text(error_message, style=me.Style(color="red"))
            return 

        with me.box(style=section_style.extend(text_align="center", padding=me.Padding.all(20), margin=me.Margin(bottom=20))):
            me.text("Overall Summary Statistics", type="headline-2", style=me.Style(margin=me.Margin(bottom=15)))
            total_reviews_count = len(all_reviews_data)
            total_restaurants_count = len(restaurants_map_data) 
            total_cities_count = len(city_names)
            with me.box(style=me.Style(display="flex", justify_content="space-around", flex_wrap="wrap")):
                me.text(f"Total Reviews: {total_reviews_count}", style=me.Style(font_size="1.2em", margin=me.Margin.all(10)))
                me.text(f"Unique Restaurants: {total_restaurants_count}", style=me.Style(font_size="1.2em", margin=me.Margin.all(10)))
                me.text(f"Unique Cities: {total_cities_count}", style=me.Style(font_size="1.2em", margin=me.Margin.all(10)))

        top_pros, top_cons, average_restaurant_ratings, reviews_over_time_chart_data = process_review_data(all_reviews_data)

        # Section: Top Pros and Cons Charts (Side-by-Side)
        with me.box(style=me.Style(display="flex", flex_direction="row", gap="20px", justify_content="space-around", margin=me.Margin(bottom=20), flex_wrap="wrap")):
            # Prepare Pros Chart Data
            pros_chart_data_for_js = None
            pros_chart_options_for_js = None
            if top_pros:
                pros_chart_data_for_js = {"labels": [item[0] for item in top_pros], "datasets": [{"label": 'Count', "data": [item[1] for item in top_pros], "backgroundColor": 'rgba(75, 192, 192, 0.6)', "borderColor": 'rgba(75, 192, 192, 1)', "borderWidth": 1}]}
                pros_chart_options_for_js = {"responsive": True, "maintainAspectRatio": False, "scales": {"y": {"beginAtZero": True, "title": {"display": True, "text": 'Count'}}, "x": {"title": {"display": True, "text": 'Pros'}}}, "plugins": {"legend": {"display": False}, "title": {"display": True, "text": 'Overall Top 10 Review Pros'}}}
            
            render_chart_section(
                title_text="Overall Top 10 Review Pros",
                chart_canvas_id="overallProsChartCanvas",
                chart_type="bar",
                chart_data_for_js=pros_chart_data_for_js,
                chart_options_for_js=pros_chart_options_for_js,
                data_exists=bool(top_pros),
                custom_section_style=me.Style(flex_basis="45%", min_width="300px")
            )
            
            # Prepare Cons Chart Data
            cons_chart_data_for_js = None
            cons_chart_options_for_js = None
            if top_cons:
                cons_chart_data_for_js = {"labels": [item[0] for item in top_cons], "datasets": [{"label": 'Count', "data": [item[1] for item in top_cons], "backgroundColor": 'rgba(255, 99, 132, 0.6)', "borderColor": 'rgba(255, 99, 132, 1)', "borderWidth": 1}]}
                cons_chart_options_for_js = {"responsive": True, "maintainAspectRatio": False, "scales": {"y": {"beginAtZero": True, "title": {"display": True, "text": 'Count'}}, "x": {"title": {"display": True, "text": 'Cons'}}}, "plugins": {"legend": {"display": False}, "title": {"display": True, "text": 'Overall Top 10 Review Cons'}}}

            render_chart_section(
                title_text="Overall Top 10 Review Cons",
                chart_canvas_id="overallConsChartCanvas",
                chart_type="bar",
                chart_data_for_js=cons_chart_data_for_js,
                chart_options_for_js=cons_chart_options_for_js,
                data_exists=bool(top_cons),
                custom_section_style=me.Style(flex_basis="45%", min_width="300px")
            )

        # Prepare Top Rated Restaurants Chart Data
        ratings_chart_data_for_js = None
        ratings_chart_options_for_js = None
        data_exists_for_ratings = bool(average_restaurant_ratings and len(average_restaurant_ratings) > 0)
        if data_exists_for_ratings:
            sorted_ratings = sorted(average_restaurant_ratings.items(), key=lambda item: (-item[1], item[0]))[:10]
            ratings_chart_data_for_js = {"labels": [item[0] for item in sorted_ratings], "datasets": [{"label": 'Average Rating', "data": [item[1] for item in sorted_ratings], "backgroundColor": 'rgba(54, 162, 235, 0.6)', "borderColor": 'rgba(54, 162, 235, 1)', "borderWidth": 1}]}
            ratings_chart_options_for_js = {"responsive": True, "maintainAspectRatio": False, "scales": {"y": {"beginAtZero": True, "max": 5, "title": {"display": True, "text": 'Average Rating (out of 5)'}}, "x": {"title": {"display": True, "text": 'Restaurant (UI Display Name)'}}}, "plugins": {"legend": {"display": False}, "title": {"display": True, "text": 'Top 10 Highest Rated Restaurants'}}}

        render_chart_section(
            title_text="Top 10 Highest Rated Restaurants",
            chart_canvas_id="overallRatingsChartCanvas",
            chart_type="bar",
            chart_data_for_js=ratings_chart_data_for_js,
            chart_options_for_js=ratings_chart_options_for_js,
            data_exists=data_exists_for_ratings
        )
        
        # Map of All Restaurants
        with me.box(style=section_style):
            me.text("Map of All Restaurants", type="headline-3", style=me.Style(text_align="center", margin=me.Margin(bottom=15)))
            if restaurants_map_data:
                map_data_json_str = json.dumps(restaurants_map_data) 
                map_html = get_map_embed_html(
                    map_element_id="generalMapDiv",
                    map_data_js_variable_name="generalPageMapData", 
                    map_data_json_str=map_data_json_str,               
                    map_init_function_name="initGeneralPageMap",   
                    google_map_id_string="GENERAL_DEMO_MAP_ID", 
                    gmaps_api_key=GMAPS_API_KEY 
                )
                me.embed_html(map_html)
            else:
                with me.box(style=map_placeholder_style): 
                    me.text("No map data available.", style=NO_DATA_STYLE)

        # Prepare Review Trends Chart Data
        trends_chart_data_for_js = None
        trends_chart_options_for_js = None
        data_exists_for_trends = bool(reviews_over_time_chart_data and reviews_over_time_chart_data.get('labels') and len(reviews_over_time_chart_data['labels']) > 0)
        if data_exists_for_trends:
            trends_chart_data_for_js = {{ "labels": reviews_over_time_chart_data['labels'], "datasets": [ {{"label": 'Number of Reviews', "data": reviews_over_time_chart_data['review_counts'], "borderColor": 'rgba(255, 159, 64, 1)', "backgroundColor": 'rgba(255, 159, 64, 0.5)', "yAxisID": 'y'}}, {{"label": 'Average Rating', "data": reviews_over_time_chart_data['average_ratings'], "borderColor": 'rgba(153, 102, 255, 1)', "backgroundColor": 'rgba(153, 102, 255, 0.5)', "yAxisID": 'y1'}} ]}}
            trends_chart_options_for_js = {{ "responsive": True, "maintainAspectRatio": False, "interaction": {{"mode": 'index', "intersect": False}}, "stacked": False, "plugins": {{"title": {{"display": True, "text": 'Review Trends Over Time'}}, "tooltip": {{"mode": 'index', "intersect": False}}}}, "scales": {{"x": {{"type": 'time', "time": {{"unit": 'month', "parser": 'YYYY-MM', "displayFormats": {{"month": 'MMM YYYY'}}}}, "title": {{"display": True, "text": 'Month'}}}}, "y": {{"type": 'linear', "display": True, "position": 'left', "title": {{"display": True, "text": 'Number of Reviews'}}, "beginAtZero": True}}, "y1": {{"type": 'linear', "display": True, "position": 'right', "title": {{"display": True, "text": 'Average Rating (out of 5)'}}, "beginAtZero": True, "max": 5, "grid": {{"drawOnChartArea": False}}}} }} }}

        render_chart_section(
            title_text="Overall Review Trends Over Time",
            chart_canvas_id="overallTimeSeriesChartCanvas",
            chart_type="line",
            chart_data_for_js=trends_chart_data_for_js,
            chart_options_for_js=trends_chart_options_for_js,
            data_exists=data_exists_for_trends
        )


# Event Handlers for detailed_filter_page()
def on_select_city_change(e: me.SelectSelectionChangeEvent):
    """Handles city selection change event for the detailed filter page."""
    me.state.selected_city = e.value
    
    if e.value == "All Cities":
        if "all_reviews_data" in me.state() and me.state.all_reviews_data: 
            unique_restaurants = sorted(list(set(r['ui_display_name'] for r in me.state.all_reviews_data if r.get('ui_display_name'))))
        else:
            unique_restaurants = []
    else:
        if "all_reviews_data" in me.state() and me.state.all_reviews_data: 
            unique_restaurants = sorted(list(set(
                r['ui_display_name'] for r in me.state.all_reviews_data 
                if r.get('city') == e.value and r.get('ui_display_name')
            )))
        else:
            unique_restaurants = []
    
    processed_resto_options = ["All Restaurants"] + unique_restaurants
    me.state.restaurant_names_options = [me.SelectOption(label=r, value=r) for r in processed_resto_options]
    me.state.selected_restaurant_name = "All Restaurants" 

def on_select_restaurant_change(e: me.SelectSelectionChangeEvent):
    """Handles restaurant selection change event for the detailed filter page."""
    me.state.selected_restaurant_name = e.value

# Detailed Filtering Page: Allows users to filter reviews by city and restaurant.
@me.page(path="/detailed", title="Restaurant Review Dashboard - Detailed View")
def detailed_filter_page(): 
    """Renders the detailed filter page with interactive controls and filtered views."""
    me.style(r"""
    body {{
        font-family: sans-serif;
        margin: 0; 
        padding: 0; 
        background-color: #f4f4f4;
        color: #333;
    }}
    """)

    me.html(r"""
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment@2.29.1/moment.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@1.0.0/dist/chartjs-adapter-moment.min.js"></script>
    """)
    
    if "selected_city" not in me.state():
      me.state.selected_city = "All Cities"
    if "selected_restaurant_name" not in me.state():
      me.state.selected_restaurant_name = "All Restaurants"
    if "all_reviews_data" not in me.state(): 
      me.state.all_reviews_data = []
    if "city_names_options" not in me.state(): 
      me.state.city_names_options = []
    if "restaurant_names_options" not in me.state(): 
      me.state.restaurant_names_options = []
    if "restaurants_map_data_state" not in me.state(): 
      me.state.restaurants_map_data_state = []
    if "error_message_state" not in me.state(): 
      me.state.error_message_state = None
    if "data_fully_loaded" not in me.state(): 
      me.state.data_fully_loaded = False

    render_nav() 

    with me.box(style=me.Style(max_width="1200px", margin=me.Margin.symmetric(horizontal="auto"), padding=me.Padding.all(20))):
        if not me.state.data_fully_loaded: 
            all_reviews, cities, map_data, error_msg = fetch_processed_data()
            me.state.all_reviews_data = all_reviews 
            me.state.restaurants_map_data_state = map_data 
            me.state.error_message_state = error_msg

            processed_city_options = ["All Cities"] + sorted(list(set(cities)))
            me.state.city_names_options = [me.SelectOption(label=city, value=city) for city in processed_city_options]
            
            if all_reviews:
                unique_restaurants = sorted(list(set(r['ui_display_name'] for r in all_reviews if r.get('ui_display_name'))))
            else:
                unique_restaurants = []
            processed_resto_options = ["All Restaurants"] + unique_restaurants
            me.state.restaurant_names_options = [me.SelectOption(label=r, value=r) for r in processed_resto_options]
            
            me.state.data_fully_loaded = True

        me.text("Burger King Review Dashboard - Detailed Filters", type="headline-1", style=me.Style(text_align="center", margin=me.Margin(bottom=20)))

        if "error_message_state" in me.state() and me.state.error_message_state:
            with me.box(style=me.Style(
                background="#ffdddd", 
                border=me.Border.all(me.BorderSide(color="red", width=1)),
                padding=me.Padding.all(15),
                margin=me.Margin(bottom=20),
                border_radius=5,
                text_align="center"
            )):
                me.text("Data Loading Error:", weight="bold", style=me.Style(color="red"))
                me.text(me.state.error_message_state, style=me.Style(color="red"))

        with me.box(style=me.Style(padding=me.Padding.all(15), margin=me.Margin(bottom=25), background="#fff", border_radius=8, box_shadow="0 2px 4px rgba(0,0,0,0.1)", display="flex", flex_direction="row", justify_content="center", align_items="center", gap="20px", flex_wrap="wrap")):
            me.text("Filter by City:")
            me.select(
                label="Select City", 
                options=me.state.city_names_options,
                value=me.state.selected_city,
                on_selection_change=on_select_city_change,
                style=me.Style(width="200px")
            )
            me.text("Filter by Restaurant:")
            me.select(
                label="Select Restaurant",
                options=me.state.restaurant_names_options,
                value=me.state.selected_restaurant_name,
                on_selection_change=on_select_restaurant_change,
                style=me.Style(width="300px")
            )

        current_reviews_for_display = []
        if "all_reviews_data" in me.state() and me.state.all_reviews_data: 
            reviews_to_filter = me.state.all_reviews_data
            if me.state.selected_city != "All Cities":
                reviews_to_filter = [
                    r for r in reviews_to_filter if r.get('city') == me.state.selected_city
                ]
            
            if me.state.selected_restaurant_name != "All Restaurants":
                reviews_to_filter = [
                    r for r in reviews_to_filter if r.get('ui_display_name') == me.state.selected_restaurant_name
                ]
            current_reviews_for_display = reviews_to_filter
        
        top_pros_detailed, top_cons_detailed, avg_ratings_detailed, time_series_detailed = process_review_data(current_reviews_for_display)

        with me.box(style=section_style): 
            me.text("Restaurant Performance Map", type="headline-3", style=me.Style(text_align="center"))
            if 'restaurants_map_data_state' in me.state() and me.state.restaurants_map_data_state:
                map_data_json_str = json.dumps(me.state.restaurants_map_data_state)
                map_html = get_map_embed_html(
                    map_element_id="detailedMapDiv",
                    map_data_js_variable_name="detailedPageMapData", 
                    map_data_json_str=map_data_json_str,                
                    map_init_function_name="initDetailedPageMap",    
                    google_map_id_string="DETAILED_DEMO_MAP_ID", 
                    gmaps_api_key=GMAPS_API_KEY
                )
                me.embed_html(map_html)
            else:
                with me.box(style=map_placeholder_style):
                    me.text("No restaurant location data available to display on map.", style=NO_DATA_STYLE)

        # Section: Pros and Cons Charts for filtered data
        with me.box(style=me.Style(display="flex", flex_direction="row", gap="20px", justify_content="space-around", margin=me.Margin(bottom=20), flex_wrap="wrap")):
            # Prepare Filtered Pros Chart Data
            filtered_pros_chart_data = None
            filtered_pros_chart_options = None
            if top_pros_detailed:
                filtered_pros_chart_data = {{"labels": [item[0] for item in top_pros_detailed], "datasets": [{"label": 'Count', "data": [item[1] for item in top_pros_detailed], "backgroundColor": 'rgba(75, 192, 192, 0.6)', "borderColor": 'rgba(75, 192, 192, 1)', "borderWidth": 1}]}}
                filtered_pros_chart_options = {{"responsive": True, "maintainAspectRatio": False, "scales": {{"y": {{"beginAtZero": True, "title": {{"display": True, "text": 'Count'}}}}, "x": {{"title": {{"display": True, "text": 'Pros'}}}}}}, "plugins": {{"legend": {{"display": False}}, "title": {{"display": True, "text": 'Top 10 Review Pros (Filtered)'}}}}}}
            
            render_chart_section(
                title_text="Top 10 Review Pros (Filtered)",
                chart_canvas_id="detailedProsChartCanvas", 
                chart_type="bar",
                chart_data_for_js=filtered_pros_chart_data,
                chart_options_for_js=filtered_pros_chart_options,
                data_exists=bool(top_pros_detailed),
                custom_section_style=me.Style(flex_basis="45%", min_width="300px")
            )
            
            # Prepare Filtered Cons Chart Data
            filtered_cons_chart_data = None
            filtered_cons_chart_options = None
            if top_cons_detailed:
                filtered_cons_chart_data = {{"labels": [item[0] for item in top_cons_detailed], "datasets": [{"label": 'Count', "data": [item[1] for item in top_cons_detailed], "backgroundColor": 'rgba(255, 99, 132, 0.6)', "borderColor": 'rgba(255, 99, 132, 1)', "borderWidth": 1}]}}
                filtered_cons_chart_options = {{"responsive": True, "maintainAspectRatio": False, "scales": {{"y": {{"beginAtZero": True, "title": {{"display": True, "text": 'Count'}}}}, "x": {{"title": {{"display": True, "text": 'Cons'}}}}}}, "plugins": {{"legend": {{"display": False}}, "title": {{"display": True, "text": 'Top 10 Review Cons (Filtered)'}}}}}}

            render_chart_section(
                title_text="Top 10 Review Cons (Filtered)",
                chart_canvas_id="detailedConsChartCanvas", 
                chart_type="bar",
                chart_data_for_js=filtered_cons_chart_data,
                chart_options_for_js=filtered_cons_chart_options,
                data_exists=bool(top_cons_detailed),
                custom_section_style=me.Style(flex_basis="45%", min_width="300px")
            )
        
        # Prepare Filtered Average Ratings Chart Data
        filtered_ratings_chart_data = None
        filtered_ratings_chart_options = None
        data_exists_for_filtered_ratings = bool(avg_ratings_detailed and len(avg_ratings_detailed) > 0)
        if data_exists_for_filtered_ratings:
            sorted_ratings_detailed = sorted(avg_ratings_detailed.items(), key=lambda item: (-item[1], item[0]))
            filtered_ratings_chart_data = {{"labels": [item[0] for item in sorted_ratings_detailed], "datasets": [{"label": 'Average Rating', "data": [item[1] for item in sorted_ratings_detailed], "backgroundColor": 'rgba(54, 162, 235, 0.6)', "borderColor": 'rgba(54, 162, 235, 1)', "borderWidth": 1}]}}
            filtered_ratings_chart_options = {{"responsive": True, "maintainAspectRatio": False, "scales": {{"y": {{"beginAtZero": True, "max": 5, "title": {{"display": True, "text": 'Average Rating (out of 5)'}}}}, "x": {{"title": {{"display": True, "text": 'Restaurant'}}}}}}, "plugins": {{"legend": {{"display": False}}, "title": {{"display": True, "text": 'Average Restaurant Ratings (Filtered)'}}}}}}

        render_chart_section(
            title_text="Average Restaurant Ratings (Filtered)",
            chart_canvas_id="detailedRatingsChartCanvas", 
            chart_type="bar",
            chart_data_for_js=filtered_ratings_chart_data,
            chart_options_for_js=filtered_ratings_chart_options,
            data_exists=data_exists_for_filtered_ratings
        )

        # Prepare Filtered Time Series Chart Data
        filtered_time_series_data = None
        filtered_time_series_options = None
        data_exists_for_filtered_trends = bool(time_series_detailed and time_series_detailed.get('labels') and len(time_series_detailed['labels']) > 0)
        if data_exists_for_filtered_trends:
            filtered_time_series_data = {{"labels": time_series_detailed['labels'], "datasets": [ {{"label": 'Number of Reviews', "data": time_series_detailed['review_counts'], "borderColor": 'rgba(255, 159, 64, 1)', "backgroundColor": 'rgba(255, 159, 64, 0.5)', "yAxisID": 'y'}}, {{"label": 'Average Rating', "data": time_series_detailed['average_ratings'], "borderColor": 'rgba(153, 102, 255, 1)', "backgroundColor": 'rgba(153, 102, 255, 0.5)', "yAxisID": 'y1'}} ]}}
            filtered_time_series_options = {{"responsive": True, "maintainAspectRatio": False, "interaction": {{"mode": 'index', "intersect": False}}, "plugins": {{"title": {{"display": True, "text": 'Review Trends Over Time (Filtered)'}}, "tooltip": {{"mode": 'index', "intersect": False}}}}, "scales": {{"x": {{"type": 'time', "time": {{"unit": 'month', "parser": 'YYYY-MM', "displayFormats": {{"month": 'MMM YYYY'}}}}, "title": {{"display": True, "text": 'Month'}}}}, "y": {{"type": 'linear', "display": True, "position": 'left', "title": {{"display": True, "text": 'Number of Reviews'}}, "beginAtZero": True}}, "y1": {{"type": 'linear', "display": True, "position": 'right', "title": {{"display": True, "text": 'Average Rating (out of 5)'}}, "beginAtZero": True, "max": 5, "grid": {{"drawOnChartArea": False}}}}}}}}
        
        render_chart_section(
            title_text="Review Trends Over Time (Filtered)",
            chart_canvas_id="detailedTimeSeriesChartCanvas", 
            chart_type="line",
            chart_data_for_js=filtered_time_series_data,
            chart_options_for_js=filtered_time_series_options,
            data_exists=data_exists_for_filtered_trends
        )
        
        # Section: Individual Reviews (Filtered) - This section remains as it's not a chart.
        with me.box(style=section_style): 
            review_section_title = "Individual Reviews"
            if me.state.selected_restaurant_name and me.state.selected_restaurant_name != "All Restaurants":
                review_section_title += f" for {me.state.selected_restaurant_name}"
            elif me.state.selected_city and me.state.selected_city != "All Cities": 
                review_section_title += f" in {me.state.selected_city}"
            review_section_title += f" ({len(current_reviews_for_display)} matching)"
            
            me.text(review_section_title, type="headline-2", style=me.Style(text_align="center", margin=me.Margin(bottom=20)))

            if current_reviews_for_display:
                sample_reviews_to_show = current_reviews_for_display[:5]
                for review in sample_reviews_to_show:
                    with me.box(style=me.Style(
                        border=me.Border.all(me.BorderSide(width=1, color="#eee")), 
                        padding=me.Padding.all(15),
                        margin=me.Margin(bottom=15), 
                        border_radius=6, 
                        box_shadow="0 1px 3px rgba(0,0,0,0.05)" 
                    )):
                        restaurant_name_to_display = review.get('ui_display_name', review.get('display_name', 'N/A'))
                        me.text(restaurant_name_to_display, type="headline-4", style=me.Style(color="#16a085", margin=me.Margin(bottom=5)))

                        review_date_str = 'N/A'
                        if review.get('review_datetime'):
                            review_dt = review['review_datetime']
                            if isinstance(review_dt, datetime):
                                review_date_str = review_dt.strftime('%Y-%m-%d') 
                            elif isinstance(review_dt, str): 
                                try:
                                    parsed_dt = datetime.fromisoformat(review_dt)
                                    review_date_str = parsed_dt.strftime('%Y-%m-%d') 
                                except ValueError:
                                    review_date_str = review_dt 
                            
                        me.text(
                            f"Rating: {review.get('review_rating', 'N/A')} ★ | Date: {review_date_str}",
                            style=me.Style(font_size="0.9em", color="#555", margin=me.Margin(bottom=8))
                        )

                        if review.get('review_text'):
                            me.text(review['review_text'], style=me.Style(margin=me.Margin(bottom=10), font_size="0.95em", line_height="1.4"))

                        if review.get('review_pros') or review.get('review_cons'):
                            with me.box(style=me.Style(display="flex", flex_direction="row", gap="15px", margin=me.Margin(top=10))):
                                if review.get('review_pros'):
                                    with me.box(style=me.Style(flex_basis="50%")):
                                        me.text("Pros:", weight="bold", style=me.Style(color="#27ae60"))
                                        pros_content = review['review_pros']
                                        if isinstance(pros_content, str) and pros_content.strip():
                                            me.text(f"• {pros_content.strip()}", style=me.Style(font_size="0.9em"))
                                        elif isinstance(pros_content, list):
                                            for pro_item in pros_content:
                                                if pro_item and pro_item.strip():
                                                    me.text(f"• {pro_item.strip()}", style=me.Style(font_size="0.9em"))
                                
                                if review.get('review_cons'):
                                    with me.box(style=me.Style(flex_basis="50%")):
                                        me.text("Cons:", weight="bold", style=me.Style(color="#c0392b"))
                                        cons_content = review['review_cons']
                                        if isinstance(cons_content, str) and cons_content.strip():
                                            me.text(f"• {cons_content.strip()}", style=me.Style(font_size="0.9em"))
                                        elif isinstance(cons_content, list):
                                            for con_item in cons_content:
                                                if con_item and con_item.strip():
                                                    me.text(f"• {con_item.strip()}", style=me.Style(font_size="0.9em"))
            else:
                me.text(
                    "No individual reviews to display for the current selection.",
                    style=NO_DATA_STYLE.extend(height="auto") 
                )
