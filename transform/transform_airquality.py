"""Transform - Air Quality Data"""
import pandas as pd

def transform_airquality_data(df_raw):
    """Transforme en moyennes mensuelles"""
    variables = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]
    
    if df_raw["time"].dtype != 'datetime64[ns]':
        df_raw["time"] = pd.to_datetime(df_raw["time"])
    
    df_raw["month"] = df_raw["time"].dt.to_period("M")
    
    monthly_avg = df_raw.groupby(["country", "year", "month"])[variables].mean().reset_index()
    monthly_avg["month"] = monthly_avg["month"].astype(str)
    
    for var in variables:
        if var in monthly_avg.columns:
            monthly_avg[var] = monthly_avg[var].round(2)
    
    return monthly_avg
