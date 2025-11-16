import polars as pl
import sys

# 3.Aggregation Processing
LOAD_FILE = "cleaned.parquet"
agg_df = pl.read_parquet(LOAD_FILE)

#1. Daily average close price per ticket

agg1_df =(
    agg_df.group_by("ticker","trade_date")
    .agg(
        pl.col("close_price").mean().alias("daily_avg_close_price")
        )
        .sort("ticker", "trade_date")
)
agg1_df.write_parquet("agg1_daily_avg_close_price.parquet")
print(f"Aggregation 1 (Daily Avg Close Price) saved to agg1_daily_avg_close_price.parquet. Shape:{agg1_df.shape}")



#2. Average Volume by Sector
agg2_df =(
    agg_df.group_by("sector")
    .agg(
        #Calculate mean volume and cast back to Int64(volume is often integer)
        pl.col("volume").mean().cast(pl.Int64).alias("avg_volume")
    )
    .sort("avg_volume", descending = True)
)
agg2_df.write_parquet("agg2_avg_volume_by_sector.parquet")
print(f"Aggregation 2 (Avg Volume by Sector) saved to agg2_avg_volume_by_sector.parquet. Shape: {agg2_df.shape}")

#3.Simple daily return per ticket
agg3_df = (
    agg_df
    #Sort by ticker and date is mandatory before calculating shift
    .sort("ticker", "trade_date")
    .with_columns(
        #Use the correct column name 'close_price'
        pl.col("close_price").shift(1).alias("prev_close_price")    
    )
    .with_columns(
        (
            (pl.col("close_price") / pl.col("prev_close_price")) -1
        )
        .alias("simple_daily_return")
        .cast(pl.Float64)
    )
    .drop("prev_close_price")
    #Remove rows where the return is NULL (the first day for each ticker)
    .filter(pl.col("simple_daily_return").is_not_null())
)
agg3_df.write_parquet("agg3_simple_daily_return.parquet")
print(f" Aggregation 3 (Simple Daily Return) saved to agg3_simple_daily_return.parquet. Shape: {agg3_df.shape}")
# print(agg3_df.head(3))

print("\n--- Aggregation complete. Three Parquet files created. ---")

