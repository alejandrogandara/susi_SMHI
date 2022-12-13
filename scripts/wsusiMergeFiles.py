import pandas as pd

entryPoint = 'https://opendata-download-metobs.smhi.se/api'
version = 1.0
parameter = 9
station = 188790
ext = 'csv'

#https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/9/station/188790/period/corrected-archive/data.csv



#Syntax
# GET /api/version/{version}/parameter/{parameter}.{ext}?measuringStations={measuringStations}

text = f"{entryPoint}/version/{version}/parameter/{parameter}/station/{station}/period/corrected-archive/data.{ext}"
print(text)
