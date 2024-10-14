import pandas as pd
import matplotlib.pyplot as plt
import os
from os import path
from python_ml_common.config import RedshiftConfig, load_envvars
from python_ml_common.loader.redshift import RedshiftLoader
from pathlib import Path


def loader():
    # Load env vars of TURO_REDSHIFT_USER and TURO_REDSHIFT_PASSWORD
    load_envvars()
    db_config = RedshiftConfig()
    db_config.username = os.getenv("TURO_REDSHIFT_USER")
    db_config.password = os.getenv("TURO_REDSHIFT_PASSWORD")

    # Initialize RedshiftLoader
    rs = RedshiftLoader(db_config)
    
    return rs

def load_data(sql_path, loader):
    # Load data into pd.DataFrame from sql_path
    with open(sql_path, 'r') as f:
        sql = f.read()
        df = loader.load(sql)
    
    return df

def preprocess_data(df):
    #Converts columns with 'month' in their name to datetime dtype
    for col in df:
        # Check if 'month' is in the column name (case-insensitive)
        if 'month' in col.lower():
            df[col] = pd.to_datetime(df[col])
    
    return df

def preprocess_forecast_data(df):
    # Keep only rows containing forecast values for each snapshot
    df_f_only = df.loc[df.month > df.last_actual_month].reset_index(drop=True)

    # Create columns of 'forecast_month' and 'increments_from_signup'
    df_f_only['forecast_month'] = df_f_only['last_actual_month'] + pd.DateOffset(months=1)
    df_f_only['increments_from_signup'] = (df_f_only['month'].dt.year - df_f_only['forecast_month'].dt.year)*12 + (df_f_only['month'].dt.month - df_f_only['forecast_month'].dt.month) + 1

    return df_f_only

def channel_increment_index(df_increment, df_channel):
    # Compute 'increment_index' and 'channel_index'
    df_increment['increment_index'] = df_increment['nrpd_increment']/df_increment['nrpd_all']
    df_channel['channel_index'] = df_channel['nrpd_channel']/df_channel['nrpd_all']

    ## Using 'increment_index' and 'channel_index', compute 'channel_increment_index'
    df_increment_12 = df_increment.loc[df_increment.increments_from_signup <= 12].reset_index(drop=True)
    df_increment_12['increment_index_1'] = df_increment_12.groupby(['forecast_month'])['increment_index'].transform('first')

    # ratio of increment index x to increment index 1
    df_increment_12['ratio_increment_index'] = df_increment_12['increment_index']/df_increment_12['increment_index_1']

    # Multiply increment 1 index for each channel with the ratio to get 'channel_increment_index'
    df_index = pd.merge(df_channel, df_increment_12[['forecast_month', 'increments_from_signup', 'ratio_increment_index']], how = 'left', on='forecast_month')
    df_index['final_index'] = df_index['channel_index'] * df_index['ratio_increment_index']
    
    return df_index

def compute_forecast_NRPD(df_f_only, df_index):
    # Merge forecast only data with channel_increment index to compute the final forecasts
    df = pd.merge(df_f_only, df_index, how = 'left', on = ['forecast_month', 'increments_from_signup'])
    df['forecast_nrpd_by_incre_channel'] = df['nrpd']*df['final_index']

    df_final = df.loc[df.increments_from_signup <= 12, ['scenario', 'forecast_month', 'month', 'increments_from_signup', 'channels', 'nrpd', 'final_index', 'data_volume', 'forecast_nrpd_by_incre_channel']].reset_index(drop=True)

    return df_final

def save_data(df, file_path):
    file_path = Path(file_path)
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)
    
    df.to_csv(file_path, index=False)

def nrpd_index_seasonality(df):
    fig = plt.figure(figsize=(25,9))
    
    for i in df['increments_from_signup'].unique():
        plt.plot('trip_month', 'increment_index', '-', marker='.', label=i, data=df.loc[df.increments_from_signup == i])

    plt.legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0.)
    plt.show()

def nrpd_accuracy_channel_plot(df):
    channels = df.channels.unique() # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ['w_nrpd_forecast_v2', 'w_nrpd_forecast_v1', 'w_nrpd_actual']
    colors = ['green', 'lightgreen', 'blue']
    names = ['New NRPD Forecast', 'Old NRPD Forecast', 'Actual NRPD']

    _, axes = plt.subplots(num_plots, 1, figsize = (13,80))
    plt.subplots_adjust(hspace=0.5)

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot('forecast_month', metric, '--' if metric!='w_nrpd_actual' else '-', label=name, color=color, marker='.', data=df.loc[df.channels==channel])
        
        ax1 = ax.twinx()
        ax1.bar(x = 'forecast_month', height = 'data_volume_y', label = 'Num Trips (Actual)', 
                width=20, color='blue', alpha=0.2,
                data = df.loc[df.channels==channel])
        ax.set_title(channel, fontweight='bold')
        ax.set_ylabel('NRPD ($)')
        ax.set_xlabel('signup month')
        ax1.set_ylabel('# trips observed')
        
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, bbox_to_anchor=(1.1, 1), loc='upper left', borderaxespad=0.)

    plt.show()
