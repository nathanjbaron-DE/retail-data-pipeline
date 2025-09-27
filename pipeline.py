import pandas as pd
import os

# ---------- Extract ----------
def extract(store_data_path: str, extra_data_path: str) -> pd.DataFrame:
    """Extracts and merges store + extra data"""
    store_df = pd.read_csv(store_data_path)  
    extra_df = pd.read_parquet(extra_data_path)
    merged_df = store_df.merge(extra_df, on="index", how="left") 
    return merged_df


# ---------- Transform ----------
def transform(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare data for analysis"""
    clean_data = raw_data.loc[:, [
        "Store_ID", "Dept", "IsHoliday", "Weekly_Sales", "CPI", "Unemployment", "Date"
    ]]

    clean_data["Date"] = pd.to_datetime(clean_data["Date"])
    clean_data["Month"] = clean_data["Date"].dt.month
    clean_data = clean_data.drop(columns=["Date"])

    clean_data = clean_data[clean_data["Weekly_Sales"] >= 10000]

    clean_data["Unemployment"] = clean_data["Unemployment"].fillna(8.106)
    clean_data["CPI"] = clean_data["CPI"].fillna(211.096358)

    return clean_data


# ---------- Aggregate ----------
def avg_weekly_sales_per_month(cleaned_data: pd.DataFrame) -> pd.DataFrame:
    """Calculate average weekly sales per month"""
    agg_data = (
        cleaned_data
        .groupby("Month")["Weekly_Sales"]
        .mean()
        .reset_index()
        .round(2)
    )
    agg_data = agg_data.rename(columns={"Weekly_Sales": "Avg_Sales"})
    return agg_data


# ---------- Load ----------
def load(cleaned_data: pd.DataFrame, agg_data: pd.DataFrame,
         output_dir: str = "data") -> None:
    """Save cleaned and aggregated data to CSVs in the data folder"""
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Save files inside the folder
    cleaned_data.to_csv(os.path.join(output_dir, "clean_data.csv"), index=False)
    agg_data.to_csv(os.path.join(output_dir, "agg_data.csv"), index=False)


# ---------- Validate ----------
def validation(file_path: str) -> None:
    """Check if a file exists"""
    print(f"{file_path} exists? {os.path.exists(file_path)}")


# ---------- Run ETL ----------
if __name__ == "__main__":
    # All input files are expected in the data folder
    input_csv = os.path.join("data", "grocery_sales.csv")
    input_parquet = os.path.join("data", "extra_data.parquet")

    merged_df = extract(input_csv, input_parquet)
    clean_df = transform(merged_df)
    agg_df = avg_weekly_sales_per_month(clean_df)

    # Save output CSVs in the data folder
    load(clean_df, agg_df, output_dir="data")

    # Validate output files
    validation(os.path.join("data", "clean_data.csv"))
    validation(os.path.join("data", "agg_data.csv"))

    print("ETL complete")
