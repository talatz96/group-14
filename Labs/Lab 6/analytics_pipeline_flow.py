# -*- coding: utf-8 -*-
"""
Created on Sat Mar 22 15:22:38 2025

@author: Admin
"""

import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger


@task
def Read_data(file_path):
    # Step 1: Fetch Data
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = pd.read_csv(file_path)
    print(f"Data shape: {df.shape}")
    return df

@task
def Validate_data(df):
    # Step 2: Validate Data
    print("Validating data...")
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean

@task
def Transform_data(df):
    # Step 3: Transform Data
    print("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
    return df

@task
def Get_Summary(df):
    # Step 4: Generate Analytics Report
    print("Generating analytics report...")
    summary = df.describe()
    summary.to_csv("analytics_summary.csv")
    print("Summary statistics saved to data/analytics_summary.csv")
    return summary


@task
def Make_Plot(df):
    # Step 5: Create a Histogram for Sales Distribution
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        print("Sales histogram saved to data/sales_histogram.png")

    print("Analytics pipeline completed.")


@flow
def analytics_pipeline():
    file_path = "analytics_data.csv"
    summary_output = "analytics_summary.csv"
    histogram_output = "sales_histogram.png"

    df = Read_data(file_path)
    df_clean = Validate_data(df)
    df_transformed = Transform_data(df_clean)
    
    Get_Summary(df_transformed)
    Make_Plot(df_transformed)




if __name__ == "__main__":
    analytics_pipeline()

