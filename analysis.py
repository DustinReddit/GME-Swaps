import os
import pandas as pd
from tqdm import tqdm

PROCESSED_PATH = (
    r"./output/processed"  # path to folder where you want processed reports to save
)

# Read in OPEN_SWAPS.csv

open_swaps = pd.read_csv(
    os.path.join(PROCESSED_PATH, "OPEN_SWAPS.csv"),
    dtype=str,
)

# Remove any blank columns with no data for convenience
open_swaps = open_swaps.dropna(axis=1, how="all")

# Remove duplicate rows
open_swaps = open_swaps.drop_duplicates()

print(open_swaps)

# For rows that have a value in "Dissemination Identifier", a blank value in "Original Dissemination Identifier", and an "Action type" of "NEWT"
#   copy the "Dissemination Identifier" value to the "Original Dissemination Identifier" column
open_swaps.loc[
    (open_swaps["Original Dissemination Identifier"].isna())
    & (open_swaps["Action type"] == "NEWT"),
    "Original Dissemination Identifier",
] = open_swaps["Dissemination Identifier"]

# Sort the df by "Event Timestamp"
open_swaps["Event timestamp"] = pd.to_datetime(open_swaps["Event timestamp"])
open_swaps = open_swaps.sort_values(by="Event timestamp")

most_recent_swaps = pd.DataFrame()

# Find the most recent swap for each unique Dissemination ID using vectorized operations
for unique_id in tqdm(open_swaps["Original Dissemination Identifier"].unique()):
    swap = open_swaps[open_swaps["Original Dissemination Identifier"] == unique_id]

    # Remove any rows that are not the most recent swap
    most_recent_swap = swap[swap["Event timestamp"] == swap["Event timestamp"].max()]

    most_recent_swaps = pd.concat(
        [most_recent_swaps, most_recent_swap], ignore_index=True
    )

# Sort by "Event Timestamp"
most_recent_swaps = most_recent_swaps.sort_values(by="Event timestamp")

# Copy "UPI FISN" to "Product name" if "Product name" is blank
most_recent_swaps.loc[most_recent_swaps["Product name"].isna(), "Product name"] = (
    most_recent_swaps["UPI FISN"]
)

# Drop the "UPI FISN" column
most_recent_swaps = most_recent_swaps.drop(columns=["UPI FISN"])

# Rename "Product Name" to "Product Name / UPI FISN"
most_recent_swaps = most_recent_swaps.rename(
    columns={"Product name": "Product name / UPI FISN"}
)


### THE FOLLOWING REINDEXING SHOULD ALWAYS BE DONE LAST ###
# Drop the "Dissemination Identifier" column and rename the "Original Dissemination Identifier" column to "Dissemination Identifier"
most_recent_swaps = most_recent_swaps.drop(columns=["Dissemination Identifier"])
most_recent_swaps = most_recent_swaps.rename(
    columns={"Original Dissemination Identifier": "Dissemination Identifier"}
)

# Set the index to the "Dissemination Identifier" column
most_recent_swaps = most_recent_swaps.set_index("Dissemination Identifier")


print(most_recent_swaps)

# Save the most recent swaps to a CSV file
output_filename = os.path.join(PROCESSED_PATH, "MOST_RECENT_SWAPS.csv")
most_recent_swaps.to_csv(output_filename)
