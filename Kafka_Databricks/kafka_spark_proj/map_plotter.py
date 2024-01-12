# Databricks notebook source
pip install voila


# COMMAND ----------

# MAGIC %md
# MAGIC #### Register table as Dataframe

# COMMAND ----------

df = spark.table("kafka.bus_location")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Collect table as list of row objs

# COMMAND ----------

record_list = df.collect()

# COMMAND ----------

import folium
Poland = [54.3558049,18.6071618]
poland_map = folium.Map(Poland,zoom_start=12)
html_map = poland_map._repr_html_()
displayHTML(html_map)

# COMMAND ----------

import time

def refresh_map():
    df = spark.table("kafka.bus_location")
    record_list = df.collect()
    for row in record_list:
        folium.Marker(
        location=[row.latitude, row.longitude],
        tooltip=row.vehicle_number,
        popup=row.vehicle_number,
        icon=folium.Icon(icon="cloud"),
        ).add_to(poland_map)

    html_map = poland_map._repr_html_()
    displayHTML(html_map)
    time.sleep(5)

while True:
    refresh_map()
    time.sleep(5)
    # clear_output(wait=True)  # Clear the previous map
    display(poland_map)
    

# for row in record_list:
#     folium.Marker(
#     location=[row.latitude, row.longitude],
#     tooltip=row.vehicle_number,
#     popup=row.vehicle_number,
#     icon=folium.Icon(icon="cloud"),
#     ).add_to(poland_map)

# html_map = poland_map._repr_html_()
# displayHTML(html_map)

# COMMAND ----------

import time

# Function to add a marker to the map
def add_marker():
    df = spark.table("kafka.bus_location")
    record_list = df.collect()
    for row in record_list:
        folium.Marker(
        location=[row.latitude, row.longitude],
        tooltip=row.vehicle_number,
        popup=row.vehicle_number,
        icon=folium.Icon(icon="cloud"),
        ).add_to(poland_map)

# Function to display the map in the Databricks notebook using JavaScript
def display_map(map_obj):
    html_map = map_obj._repr_html_()
    displayHTML(f'<div id="map">{html_map}</div>')

# Function to refresh the map at regular intervals using JavaScript
def refresh_map(interval_seconds):
    while True:
        add_marker()
        display_map(poland_map)
        time.sleep(interval_seconds)
        displayHTML('<script>$("#map").html("")</script>')  # Clear the previous map

# Start refreshing the map with a 5-second interval
refresh_interval = 10
refresh_map(refresh_interval)

# COMMAND ----------

import folium
import time
from random import uniform
from IPython.display import display, HTML

# Function to generate random coordinates for demonstration purposes
# def generate_random_coordinates():
#     return uniform(-90, 90), uniform(-180, 180)

# Function to create and save a new map with a marker
def create_and_save_map():
    Poland = [54.3558049,18.6071618]
    mymap = folium.Map(Poland,zoom_start=12)
    add_marker(mymap)
    file_path = "FileStore/dynamic_map.html"  # Use a path accessible by Databricks
    mymap.save(file_path)
    return file_path

# Function to add a marker to the map
def add_marker(mymap):
    # coordinates = generate_random_coordinates()
    # folium.Marker(location=coordinates).add_to(mymap)
    df = spark.table("kafka.bus_location")
    record_list = df.collect()
    for row in record_list:
        folium.Marker(
        location=[row.latitude, row.longitude],
        tooltip=row.vehicle_number,
        popup=row.vehicle_number,
        icon=folium.Icon(icon="cloud"),
        ).add_to(mymap)

# Function to display the map in the Databricks notebook using HTML
def display_map(file_path):
    display(HTML(f'<iframe src="{file_path}" width="100%" height="500"></iframe>'))

# Function to refresh the map at regular intervals
def refresh_map(interval_seconds, num_updates):
    for _ in range(num_updates):
        file_path = create_and_save_map()
        display_map(file_path)
        time.sleep(interval_seconds)

# Start refreshing the map with a 5-second interval for 5 updates
refresh_interval = 5
num_updates = 5
refresh_map(refresh_interval, num_updates)


# COMMAND ----------

dbutils.fs.ls("/)

# COMMAND ----------

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


# COMMAND ----------

import voila
from IPython.display import display

def show_dynamic_map():
  """
  Renders the Streamlit app with live map updates using Voilà.
  """
  display(voila.render(filename="dynamic_map.py"))

show_dynamic_map()


# COMMAND ----------

pip install voila

# COMMAND ----------

pip install streamlit folium streamlit_folium

# COMMAND ----------

import subprocess

# COMMAND ----------

subprocess.run(["streamlit", "run", "stremlit_app.py"])

# COMMAND ----------

# MAGIC %sh
# MAGIC streamlit run stremlit_app.py

# COMMAND ----------

