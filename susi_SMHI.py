import pandas as pd
import os
import requests
from pathlib import Path
import numpy as np

def getWeather(site, x, y, start_date, end_date, outputF, workingFolder, stations_nearby):
    output_folder = mk(outputF)
    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    wFile = checkWFile(site, x, y, start_date, end_date, output_folder)
    if wFile is not False:
        return wFile

    # setting up default variable code for the SMHI api.
    wStations = pd.DataFrame(
        {"varType": ["t_mean", "t_max", "t_min", "rainfall", "radiation", "hpa", "humidity" ],
        "stationType": ["Temp_mean", "Temp_max", "Temp_min", "Precipitation", "GlobalRad", "AirPreassure", "humidity"],
        "parameter": [2, 20, 19, 5, 11, 9, 6],
        "stations_file": ['metobs_airtemperatureMean24h_core_sites.csv', 'metobs_airTemperatureMinAndMaxOnceEveryDay_core_sites.csv', 'metobs_airTemperatureMinAndMaxOnceEveryDay_core_sites.csv', 'metobs_precipitationType24Hours_core_sites.csv', 'metobs_globalIrradians_core_sites.csv', 'metobs_airPressure_core_sites.csv', 'metobs_airHumidity_core_sites.csv']})
    # remove the AirPreassure, but the funcionality still there if needed.
    wStations = wStations.loc[~wStations['varType'].isin(['hpa'])]

    ## calculate distance matrix
    distanceMatrix = {}
    for i in wStations.index:
        path = wStations['stations_file'][i]
        stations = pd.read_csv(f'{workingFolder}/susi_SMHI/smhi_process/stations/{path}', sep=';')
        #stations = pd.read_csv(f'susi_SMHI/smhi_process/stations/{path}', sep=';')
        distanceMatrix[wStations['stationType'][i]] = calcDistanceMatrix(site, x, y, stations)

    # SMHI api
    # https://opendata.smhi.se/apidocs/metobs/index.html
    #https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/9/station/188790/period/corrected-archive/data.csv
    #version = 1.0
    #parameter = 9
    #station = 188790
    #ext = 'csv'

    #Syntax
    # GET /api/version/{version}/parameter/{parameter}.{ext}?measuringStations={measuringStations}
    try:
        sw, stReaded = integrateData(site, start_date, end_date, wStations, distanceMatrix, workingFolder, stations_nearby)
    except Exception as e: 
        print(e)
        return
    #Check nd percentage
    missing_value_df, percent_missing = missing_data(sw)
    sw.index =pd.to_datetime(sw.index)  #correct index to date

    #RESAMPLE column if nd found
    for col in missing_value_df.varType:
        if missing_value_df[missing_value_df.varType == col].percent_missing[0] > 0:
            sw[col] = sw[col].resample('1D').mean().interpolate()
            missing_value_df.at[col, 'resampled'] = True
        else:  missing_value_df.at[col, 'resampled'] = False

    # calculate columns
    sw, stReaded = calc_hPa(sw, stReaded)
    #missing_value_df, percent_missing = missing_data(sw, missing_value_df)
    summary = pd.concat([missing_value_df, stReaded.nStations], axis=1)
    summary['site'] = site

    # get the data in structure
    integratedData = pd.DataFrame({
        'OmaTunniste': "",
        'OmaIt': "",
        'OmaPohjoinen': "",
        'Kunta': site,
        'siteid': site.split('-')[0],
        'aika':  pd.to_datetime(sw.index).strftime('%Y%m%d'),
        'vuosi':  pd.to_datetime(sw.index).strftime('%Y'),
        'kk': pd.to_datetime(sw.index).strftime('%m'),
        'paiva': pd.to_datetime(sw.index).strftime('%d'),
        'longitude': x,
        'latitude': y,
        't_mean': pd.to_numeric(sw.t_mean),
        't_max': pd.to_numeric(sw.t_max),
        't_min': pd.to_numeric(sw.t_min),
        'rainfall': pd.to_numeric(sw.rainfall),
        'radiation': pd.to_numeric(sw.radiation).round(2),
        'hpa': pd.to_numeric(sw.hpa).round(2)
    })
    #write the data missing value table
    summary.to_csv(output_folder+site+'_weather'+'_summary.csv', sep=';', index=False)
    stReaded.to_csv(output_folder+site+'_weather'+'_stReaded.csv', sep=';', index=False)
    wfile = output_folder+site+'_weather.csv'
    integratedData.to_csv(wfile, sep=';', index=False)
    print('\n')
    print(summary)
    print(f"\n site {site}: \tOK -- from { start_date} to {end_date} -- ({len(sw)} days, {round((len(sw) / 365.25))} years) max {stations_nearby} stations nearby")
    #except: print(f"site {site}: \tERROR")
    return wfile

def mk(path):
    if not os.path.exists(path): os.mkdir(path)
    return path

def checkWFile(site, x, y, start_date, end_date, output_folder):
    wfile = f'{output_folder}{site}_weather.csv'
    if os.path.exists(wfile):
        weatherFile = pd.read_csv(wfile, sep=';')
        if (round(weatherFile.latitude.mean(),2) == round(y,2)) & (round(weatherFile.longitude.mean(),2) == round(x,2)):
            wfile_start_date = weatherFile.aika.min()
            wfile_end_date = weatherFile.aika.max()
            param_start_date = int(pd.to_datetime(start_date).strftime('%Y%m%d'))
            param__end_date = int(pd.to_datetime(end_date).strftime('%Y%m%d'))

            if (wfile_start_date <= param_start_date) & (wfile_end_date >= param__end_date):
                mv, pv = missing_data(weatherFile)
                print(mv.to_string(index=False))
                print(f'\nThe weather file {site} from {param_start_date} to {param__end_date} is ready to go')
                
                return wfile
    else: 
        return False

def calcDistanceMatrix(site, x, y, stations):
    stations['InputID'] = site
    stations['Distance'] = np.sqrt((stations['Longitude'] - x)**2 + (stations['Latitude'] - y)**2)
    stations['TargetID'] = stations['Id']
    dm = stations.loc[:,['InputID', 'Distance', 'TargetID']]
    dm.sort_values('Distance', inplace=True)
    dm = dm.reset_index(drop=True)
    return dm

def readData(station, parameterLabel, start_date, end_date, wStations, workingFolder, version = 1.0, ext = 'csv'):
    #print(f"{station}.{ext}")
    #print( wStations.loc[wStations.varType == parameterLabel])
    entryPoint = 'https://opendata-download-metobs.smhi.se/api'
    dumpFolder = mk(f'{workingFolder}/susi_SMHI/smhi_process/dump/')

    parameter = wStations.loc[wStations.varType == parameterLabel].parameter.item() #assign code to parameter label
    #reads the air pressure data, and returns a dataframe with daily average values
    file = f"{entryPoint}/version/{version}/parameter/{parameter}/station/{station}/period/corrected-archive/data.{ext}"

    r = requests.get(file)
    dumpFile = f"{dumpFolder}/{parameter}_{station}.{ext}"
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
        dumpFile = f"{dumpFolder}{parameter}_{station}_{status}.{ext}"
        with open(dumpFile, 'wb') as f:
            f.write(r.content)

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
    try: ds = ds.loc[(ds.index >= start_date) & (ds.index <= end_date)]
    except: 
        ds = pd.DataFrame({'Datum':start_date, parameterLabel:None})
        ds.set_index('Datum', drop=True, inplace=True)

    ds = ds[~ds.index.duplicated(keep='first')]  #delete the duplicated records

    return ds

def integrateData(site, start_date, end_date, wStations, distanceMatrix, workingFolder, stations_nearby=1):
# the function iterates on varType ["hpa", "radiation", "rainfall", "t_mean", "t_max", "t_min"]
# depends on the varType it selects the station type group, ex t_mean uses Temperature station
# then selects n stations nearby, gets the data and integrate it
    stReaded = pd.DataFrame(columns= ['varType', 'nStations'])
    stReaded.set_index('varType', drop=False ,inplace=True)

    if stations_nearby < 1: stations_nearby = 1
    stationDataFilled = {}
    for varType in wStations.varType:
        #print(f"varType {wStations[wStations.varType == varType]}, ")
        st = wStations[wStations.varType == varType].stationType.item() # gets the station type, related to varType
        dmType = distanceMatrix[st]  #filter the distance matrix by station type
        dmSite=dmType.loc[dmType["InputID"] == site].sort_values(by="Distance").head(stations_nearby)  #gets the top n nearest sations for the site
        stationData = {}
        stationDataGaps = pd.DataFrame(data = {varType: None, 'Datum':pd.date_range(start_date, end_date, freq="D").strftime('%Y-%m-%d')})
        stationDataGaps.set_index('Datum', drop=True, inplace=True)

        for stationPosition in range(stations_nearby):   #using for cycle
#        stationPosition = 0
#        while True: 
            stationID = dmSite["TargetID"].iloc[stationPosition]
            stationData[stationPosition] = readData(stationID, varType, start_date, end_date, wStations, workingFolder) # reads station data
            stationDataGaps = stationDataGaps.combine_first(stationData[stationPosition])
            if stationDataGaps.isnull().sum(axis = 1).sum() == 0: break
#            stationPosition += 1
#            if (stationPosition > stations_nearby) or (stationDataGaps.isnull().sum(axis = 1).sum() == 0): break

        stationDataFilled[varType] = stationDataGaps
        stReaded = pd.concat([stReaded, pd.DataFrame({'varType':[varType], 'nStations':[stationPosition +1]})], axis=0, ignore_index=True)
        stReaded.set_index('varType', drop=False ,inplace=True)
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
    
    #stReaded.index = stReaded.varType
    st_hpa = pd.DataFrame({'varType':['hpa'], 'nStations':['calculated']}, index=['hpa'])

    stReaded = pd.concat([st_hpa, stReaded] , axis=0, ignore_index=False)
    stReaded.set_index('varType', drop=False ,inplace=True)
    return ds, stReaded
#ttt