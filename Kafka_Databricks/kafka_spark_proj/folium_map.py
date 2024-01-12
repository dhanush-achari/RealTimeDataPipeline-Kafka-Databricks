# Databricks notebook source
import time
import folium

Poland = [54.3558049,18.6071618]
poland_map = folium.Map(Poland,zoom_start=12,width=1000,height=1000)

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

refresh_map()
# time.sleep(25)
# refresh_map()

# COMMAND ----------

