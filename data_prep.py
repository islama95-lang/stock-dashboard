import polars as pl

#URL of the dataset
DATA_URL = "https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/stock_market.csv"

#1.Load CSV
# polars supports direct loading from URLs
raw_df = pl.read_csv(DATA_URL)

print("## 1. Raw Data Inspection (Polars)")

#Show shape (rows, columns)
print(f"Shape: {raw_df.shape} (Rowas, Columns)")

#Preview first 5 rows
print("\n### preview first 5 Rows")
print(raw_df.head())

#Show data types (schema)
print("\n### Quick Schema (Data Types)")
print(raw_df.dtypes)

#Null value summary
print("\n### Null Value Counts")
#Count null values for each column
null_summary = raw_df.select(
    [pl.col(c).null_count().alias(f"{c}_null_count") for c in raw_df.columns]
)
print(null_summary)


#2. Schema normalization and data cleaning
CLEANED_FILE = "cleaned.parquet"

#Function to convert headers to snake_case
def to_snake_case(name):
    return name.strip().lower().replace(' ','_').replace('/','_').replace('.','_')

#List of values treated as missing
NULL_VALUES = ["", "NA", "N/A", "NULL", "-", "na", "n/a", "null"]
STANDARD_NULL_REPLACEMENT = None #Polars uses None as null

print("\n## 2. Schema Normalization and Data Cleaning")

(
   pl.scan_csv(DATA_URL)
    #1. Convert headers to snake_case
   # Read only the header row (n_rows = 1) to get column names quickly
   .rename({col: to_snake_case(col) for col in pl.read_csv(DATA_URL, n_rows=1).columns})
   
   
   #2. Trim whitespace and standardize strings to lowercase
   .with_columns(
       #Apply strip + lowercase to all string columns
       pl.col(pl.String).str.strip_chars().str.to_lowercase()
   )
   
   #3. Standardize missing values
   .with_columns(
       #Replace any string matching a NULL-like token with actual Null
       pl.when(pl.col(pl.String).is_in(NULL_VALUES))
       .then(STANDARD_NULL_REPLACEMENT)
       .otherwise(pl.col(pl.String))
       .name.keep()
   )
    #4. Fix date formatting
    .with_columns(
        #strict = False : return Null instead of error
      pl.col("trade_date").str.to_date("%m/%d/%Y", strict=False).alias("trade_date") 
    )
    
    #5. Cast data types for  the target schema
    .with_columns(
        #Convert price columns to float
        # Convert volume to integer
         # Convert other selected columns to string
        #(validated could be boolean or catagory, but kept as string for simplicity)
         #Ensure sector is treated as string
        pl.col("open_price", "close_price").cast(pl.Float64), pl.col("volume").cast(pl.Int64),pl.col("validated", "currency", "exchange", "notes").cast(pl.String),pl.col("sector").cast(pl.String).alias("sector"),
        
    )
    #6. Remove duplicate rows
    .unique()
    
    #7. Execute the Lazy query and write to parquet
    .collect()
    .write_parquet(CLEANED_FILE)    
)

print(f" Cleaned data saved to {CLEANED_FILE}")

#Load and inspect cleaned data
cleaned_df = pl.read_parquet(CLEANED_FILE)

print("\n### Cleaned Data Schema and Preview") 
print(cleaned_df.dtypes)
print(cleaned_df.head(3))   
    
