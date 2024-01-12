# Databricks notebook source
pip install streamlit folium streamlit_folium

# COMMAND ----------

import streamlit as st
import folium
from streamlit_folium import st_folium

# Create initial map with markers
map = folium.Map(location=[40.7484, -73.9857])  # Adjust initial location as needed
marker = folium.Marker([40.7484, -73.9857])  # Initial marker
map.add_child(marker)

# Display map with placeholders for dynamic updates
st_folium(map, width=800, height=600)

# Buttons or other widgets to trigger updates
if st.button("Update Markers"):
    # Update marker coordinates, add more markers, etc.
    new_marker = folium.Marker([44.7484, -74.9857])  # Example new marker
    map.add_child(new_marker)

    # Dynamically update the map using feature_group_to_add
    st_folium(map, zoom=12, center=[42.7484, -75.9857], feature_group_to_add=new_marker)


# COMMAND ----------

!streamlit run /databricks/python_shell/scripts/db_ipykernel_launcher.py [ARGUMENTS]

# COMMAND ----------

