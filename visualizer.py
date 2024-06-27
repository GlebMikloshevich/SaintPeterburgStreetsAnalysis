import streamlit as st
import pandas as pd

st.set_page_config(page_title="spb streets", page_icon="/home/gleb/PycharmProjects/couse/raw/map.png", layout="wide")

avg_df = pd.read_csv("/home/gleb/PycharmProjects/couse/ImageGathering/analysis/data/avg_df.csv")
sum_df = pd.read_csv("/home/gleb/PycharmProjects/couse/ImageGathering/analysis/data/sum_df.csv")

# normalization for the better map
sum_df["sum_car"] = sum_df["sum_car"] / 20
sum_df["sum_person"] = sum_df["sum_person"] / 20
sum_df["sum_bus"] = sum_df["sum_bus"] / 3

# map
st.title("Map Plot")
option = "sum_"+st.selectbox("**Select Object**", ["car", "person", "bus"])
st.map(data=sum_df, latitude="latitude", longitude="longitude", color=None, size=option, zoom=None,
       use_container_width=True)

# 1st chart
st.title("24-Hour Timeline Bar Plot by location")

street = st.selectbox("**Select Camera**", avg_df["street"].unique())
data_type = "avg_" + st.selectbox("**Select Object**", ["car", "person", "bus"], key="chart1")


filtered_df = avg_df[avg_df["street"] == street][["hour", data_type]]
hour_car_df = avg_df[avg_df["street"] == street][["hour", data_type]]
filtered_df = filtered_df.set_index("hour")[data_type]
st.bar_chart(filtered_df)

hour_car_df_html = hour_car_df.sort_values(by='hour').set_index("hour")
st.write(hour_car_df_html.T)

# 2nd chart
st.title("24-Hour Timeline Bar Plot")
data_type_24 = "avg_" + st.selectbox("**Select Object**", ["car", "person", "bus"], key="4242424242242")

df_hour_all = avg_df.copy()[["hour", data_type_24]]
df_hour_all = df_hour_all.set_index("hour")

st.bar_chart(df_hour_all)
