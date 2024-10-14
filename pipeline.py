import logging
import os
from os import path
from datetime import datetime
from utils import (
    loader,
    load_data,
    preprocess_data,
    preprocess_forecast_data,
    channel_increment_index,
    compute_forecast_NRPD,
    save_data
)

timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

CURRENT_DIR = path.dirname(path.abspath(__file__))
SQL_PATH = path.join(CURRENT_DIR, "sql")

def main():
    # initialize redshift loader
    rs = loader()

    logging.info("Loading data.")
    # Load nrpd forecast data from FP&A team
    df_nrpd_f = load_data(
        sql_path = os.path.join(SQL_PATH, "nrpd_forecast.sql"),
        loader=rs)
    
    # Load nrpd actual data by channel at increment 1
    df_nrpd_channel_a = load_data(
        sql_path = os.path.join(SQL_PATH, "nrpd_channel_index.sql"),
        loader=rs)
    
    # Load nrpd actual data by
    df_nrpd_increment_a = load_data(
        sql_path = os.path.join(SQL_PATH, "nrpd_increment_index.sql"),
        loader=rs)
    
    logging.info("Preprocessing data.")
    # Change '_month' dtypes into datetime dtype
    for d in [df_nrpd_f, df_nrpd_channel_a, df_nrpd_increment_a]:
        d = preprocess_data(d)
    
    # Keep only forecast values + create additional columns
    df_nrpd_f_only = preprocess_forecast_data(df_nrpd_f)
    
    logging.info("Computing forecasts of NRPD.")
    df_index = channel_increment_index(df_nrpd_increment_a, df_nrpd_channel_a)
    df_final = compute_forecast_NRPD(df_nrpd_f_only, df_index)


    logging.info("Saving forecast data locally.")
    local_output_path = "./outputs/{TIMESTAMP}/nrpd_forecast.csv".format(TIMESTAMP = timestamp)
    save_data(df_final, local_output_path)
    

if __name__ == "__main__":
    main()

