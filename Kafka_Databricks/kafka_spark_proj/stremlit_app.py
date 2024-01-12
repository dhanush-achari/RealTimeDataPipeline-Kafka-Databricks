# Databricks notebook source
import streamlit as st
import folium
from streamlit_folium import st_folium

# Initialize session state for map and markers
# if "map" not in st.session_state:
st.session_state.map = folium.Map(location=[54.3558049,18.6071618],zoom_start=12)  # Initial location
st.session_state.markers = []  # Store markers in a list

def update_map():
    """Updates the map with new markers and properties."""
    # Add new markers or modify existing ones
    df = spark.table("kafka.bus_location")
    record_list = df.collect()
    for row in record_list:
        st.session_state.markers.append(folium.Marker(
        location=[row.latitude, row.longitude],
        tooltip=row.vehicle_number,
        popup=row.vehicle_number,
        icon=folium.Icon(icon="cloud"),
        ))

    # Adjust map properties as needed
    st.session_state.map.zoom_start = 12
    # st.session_state.map.center = [42.7484, -75.9857]

    # Add markers to the map
    for marker in st.session_state.markers:
        st.session_state.map.add_child(marker)

    return st.session_state.map

# Display the map (callbacks ensure re-rendering when needed)
st_folium(update_map(), width=800, height=600)

# Button to trigger map updates
if st.button("Update Markers"):
    update_map()  # Call the function to update markers and map properties