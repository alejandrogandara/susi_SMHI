import pandas as pd
import os
import requests
#from pyproj import Proj

# Global definitions
entryPoint = 'https://opendata-download-metobs.smhi.se/api'

output_folder = 'weather_output/2022_12_02_weather/'
if not os.path.exists(output_folder): os.mkdir(output_folder)

date_start = '2000-01-01'
date_end = '2021-12-31'
stations_nearby = 10

# reading sites
wStations = pd.DataFrame(
    {"varType": ["t_mean", "t_max", "t_min", "rainfall", "radiation", "hpa", "humidity" ],
    "stationType": ["Temp", "Temp", "Temp", "Precipitation", "GlobalRad", "AirPreassure", "humidity"],
    "parameter": [2, 20, 19, 5, 11, 9, 6]})

#version = 1.0
#parameter = 9
#station = 188790
#ext = 'csv'

wStations = wStations.loc[~wStations['varType'].isin(['hpa'])]  # remove the AirPreassure
#wStations = wStations[wStations['varType'] == 'Temp'] #try only humidity

#reading distance matrix for Ulf Stations, the station_dm index is manual for now but shoud come from a function
# NOTA, AUTOMATIZAR LA DISTANCE MATRIX PARA EVITAR ERROR EN NOMBRES
dm_dir = "shapefiles/distance_matrix/"
station_dm = os.listdir(dm_dir)
distanceMatrix = {
"AirPreassure": pd.read_csv(dm_dir + station_dm[0], sep=',', encoding='utf-8'),
"GlobalRad": pd.read_csv(dm_dir + station_dm[1], sep=',', encoding='utf-8'),
"humidity": pd.read_csv(dm_dir + station_dm[2], sep=',', encoding='utf-8'),
"Precipitation": pd.read_csv(dm_dir + station_dm[3], sep=',', encoding='utf-8'),
'Temp': pd.read_csv(dm_dir + station_dm[4], sep=',', encoding='utf-8')

}

### manual entry of the site description
site_description = pd.read_csv("shapefiles/ulf_sites_simple.csv", sep=',', encoding='latin1')
#distanceMatrix.keys()

#"parameter": [2, 20, 19, 5, 'GlobalRLink', 9]})
# sites exracted from distance matrix (not the optimal way but it works for Uls Sites)
sites = distanceMatrix["Precipitation"]["InputID"].unique()


#sites = sites[[0]]

# SMHI api
# https://opendata.smhi.se/apidocs/metobs/index.html
#https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/9/station/188790/period/corrected-archive/data.csv

#Syntax
# GET /api/version/{version}/parameter/{parameter}.{ext}?measuringStations={measuringStations}

def readData(station, parameterLabel, date_start, date_end, version = 1.0, ext = 'csv'):
    #print(f"{station}.{ext}")
    #print( wStations.loc[wStations.varType == parameterLabel])

    parameter = wStations.loc[wStations.varType == parameterLabel].parameter.item() #assign code to parameter label
    #reads the air pressure data, and returns a dataframe with daily average values
    file = f"{entryPoint}/version/{version}/parameter/{parameter}/station/{station}/period/corrected-archive/data.{ext}"

    r = requests.get(file)
    dumpFile = f"dump/{parameter}_{station}.{ext}"
    with open(dumpFile, 'wb') as f:
        f.write(r.content)


    skp = pd.read_csv(file, sep='\t|;', engine='python', names=range(50), skip_blank_lines=False).iloc[1:20,[0]]    #read file to find the first row
        #if len(skp) == 0: return 0
    datum_skp = skp[skp[0].str.contains("Datum", na = False)].index.values[0]   #define the first row

    if parameter == 9:  #9 AirPressure
        ds = pd.read_csv(file, sep='\t|;', engine='python', skiprows=datum_skp).iloc[:,[0,2]]
        ds = ds.groupby('Datum', as_index=False).mean()
        
    elif parameter == 6:  #6 Humidity
        # the next try and except is not mandatory, it is for testing proposes.
        try: 
            ds = pd.read_csv(file, sep='\t|;', engine='python', skiprows=datum_skp).iloc[:,[0,2]]
            ds = ds.groupby('Datum', as_index=False).mean()
            status = "ok"
        except:
            print(f"error reading file {file}, ")
            status="fail"

        r = requests.get(file)
        dumpFile = f"dump/{parameter}_{station}_{status}.{ext}"
        with open(dumpFile, 'wb') as f:
            f.write(r.content)

            ##########################################################################################


    elif parameter == 11:  #11 GlobalRad
        ds = pd.read_csv(file, sep='\t|;', engine='python', skiprows=datum_skp).iloc[:,[0,2]]
        ds = ds.groupby('Datum', as_index=False).sum()  # sum watt hour to day
        ds.iloc[:,[1]] = ds.iloc[:,[1]] * 3.6  # watt to Joules
        
    else:  #rainfall and temperature
        ds = pd.read_csv(file, sep='\t|;', engine='python', skiprows=datum_skp).iloc[:,[2,3]]

    ds.columns = ['Datum', parameterLabel]  #rename headers
    ds.Datum = pd.to_datetime(ds.Datum, infer_datetime_format=True).dt.strftime('%Y-%m-%d')
    ds.set_index('Datum', drop=True, inplace=True)

    # some tyding
    try: ds = ds.loc[(ds.index >= date_start) & (ds.index <= date_end)]
    except: 
        ds = pd.DataFrame({'Datum':date_start, parameterLabel:None})
        ds.set_index('Datum', drop=True, inplace=True)

    ds = ds[~ds.index.duplicated(keep='first')]  #delete the duplicated records

    return ds

def integrateData (site, distanceMatrix, date_start, date_end, stations_nearby=1, wStations=wStations):
# the function iterates on varType ["hpa", "radiation", "rainfall", "t_mean", "t_max", "t_min"]
# depends on the varType it selects the station type group, ex t_mean uses Temperature station
# then selects n stations nearby, gets the data and integrate it
    stReaded = pd.DataFrame(columns= ['varType', 'nStations'])

    if stations_nearby < 1: stations_nearby = 1
    stationDataFilled = {}
    for varType in wStations.varType:
        #print(f"varType {wStations[wStations.varType == varType]}, ")
        st = wStations[wStations.varType == varType].stationType.item() # gets the station type, related to varType
        dmType = distanceMatrix[st]  #filter the distance matrix by station type
        dmSite=dmType.loc[dmType["InputID"] == site].sort_values(by="Distance").head(stations_nearby)  #gets the top n nearest sations for the site
        stationData = {}
        stationDataGaps = pd.DataFrame(data = {varType: None, 'Datum':pd.date_range(date_start, date_end, freq="D").strftime('%Y-%m-%d')})
        stationDataGaps.set_index('Datum', drop=True, inplace=True)

        for stationPosition in range(stations_nearby):   #using for cycle
#        stationPosition = 0
#        while True: 
            stationID = dmSite["TargetID"].iloc[stationPosition]
            stationData[stationPosition] = readData(stationID, varType, date_start, date_end) # reads station data
            stationDataGaps = stationDataGaps.combine_first(stationData[stationPosition])
            if stationDataGaps.isnull().sum(axis = 1).sum() == 0: break
#            stationPosition += 1
#            if (stationPosition > stations_nearby) or (stationDataGaps.isnull().sum(axis = 1).sum() == 0): break

        stationDataFilled[varType] = stationDataGaps

        stReaded = pd.concat([stReaded, pd.DataFrame({'varType':[varType], 'nStations':[stationPosition +1]})], axis=0, ignore_index=True)
        print(f"{varType}: {stationPosition +1}", end=' ')

        #stationDataGaps.to_csv(output_folder+site+'_'+varType+'_weather.csv', sep=';', index=False)



    ## hacer join de los stationDataFilled para hacer el dataset final.
    #return stationDataFilled
    stationDataConcat = pd.concat(stationDataFilled, axis=1).droplevel(0, axis=1)
    #stationDataConcat = stationDataConcat.loc[:,~stationDataConcat.columns.duplicated()]
    return stationDataConcat, stReaded
    # return stationDataFilled  #for testing proposes

def missing_data (sw, mv=None):
    # mv: missing_value_df
    check = 0 if mv is None else 1
    percent_missing = sw.isnull().sum() * 100 / len(sw)
    if check == 1:
        tmv = pd.DataFrame({'varType': sw.columns,
                            'percent_missing': percent_missing})
        tmv.set_index('varType', drop=False ,inplace=True)
        tmv = tmv[~tmv.index.isin(mv.index)]
        mv = pd.concat([mv, tmv],ignore_index=False)
    else:
        mv = pd.DataFrame({'varType': sw.columns,
                'percent_missing': percent_missing})
        mv.set_index('varType', drop=False, inplace=True)
    return mv, percent_missing  


def vaporPressure (tempC, RH):
    from math import exp
    to_hecto = 0.01
    es = 611* exp( (17.27 * tempC) / ( 237.2 + tempC))
    e = (es * RH)/ 100
    e_hPa = e * to_hecto
    return e_hPa

def calc_hPa(ds, stReaded):
    # calculate HPA
    #ds['hpa'] = 1
    ds['hpa'] = ds.apply(lambda ds : vaporPressure(ds['t_mean'], ds['humidity']), axis=1)
    
    stReaded.index = stReaded.varType
    stReaded = pd.concat([stReaded, pd.DataFrame({'varType':['hpa'], 'nStations':['calculated']})], axis=0, ignore_index=True)
    return ds, stReaded
