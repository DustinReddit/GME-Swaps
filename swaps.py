import pandas as pd
import numpy as np
import glob
import requests
import os
from zipfile import ZipFile
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Define some configuration variables
OUTPUT_PATH = r"./output"  # path to folder where you want filtered reports to save
MAX_WORKERS = 16  # number of threads to use for downloading and filtering

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Generate daily dates from two years ago to today
start = datetime.datetime.today() - datetime.timedelta(days=730)
end = datetime.datetime.today()
dates = [start + datetime.timedelta(days=i) for i in range((end - start).days + 1)]

# Generate filenames for each date
filenames = [
    f"SEC_CUMULATIVE_EQUITIES_{year}_{month}_{day}.zip"
    for year, month, day in [
        (date.strftime("%Y"), date.strftime("%m"), date.strftime("%d"))
        for date in dates
    ]
]


def download_and_filter(filename):
    url = f"https://pddata.dtcc.com/ppd/api/report/cumulative/sec/{filename}"
    req = requests.get(url)

    if req.status_code != 200:
        print(f"Failed to download {url}")
        return

    with open(filename, "wb") as f:
        f.write(req.content)

    # Extract csv from zip
    with ZipFile(filename, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        zip_ref.extractall()

    # Load content into dataframe
    df = pd.read_csv(csv_filename, low_memory=False, on_bad_lines="skip")

    # Perform some filtering and restructuring of pre 12/04/22 reports
    if "Primary Asset Class" in df.columns or "Action Type" in df.columns:
        df = df[
            df["Underlying Asset ID"].str.contains(
                "GME.N|GME.AX|US36467W1099|36467W109", na=False
            )
        ]
    else:
        df = df[
            df["Underlier ID-Leg 1"].str.contains(
                "GME.N|GME.AX|US36467W1099|36467W109", na=False
            )
        ]

    # Save the dataframe as CSV
    output_filename = os.path.join(OUTPUT_PATH, f"{csv_filename}")
    df.to_csv(output_filename, index=False)

    # Delete original downloaded files
    os.remove(filename)
    os.remove(csv_filename)


tasks = []
for filename in filenames:
    tasks.append(executor.submit(download_and_filter, filename))

for task in tqdm(as_completed(tasks), total=len(tasks)):
    pass

files = glob.glob(OUTPUT_PATH + "/" + "*")

# Ignore "filtered.csv" file
files = [file for file in files if "filtered" not in file]


def filter_merge():
    master = pd.DataFrame()  # Start with an empty dataframe

    for file in files:
        df = pd.read_csv(file, low_memory=False)

        # Skip file if the dataframe is empty, meaning it contained only column names
        if df.empty:
            continue

        # Check if there is a column named "Dissemination Identifier"
        if "Dissemination Identifier" not in df.columns:
            # Rename "Dissemintation ID" to "Dissemination Identifier" and "Original Dissemintation ID" to "Original Dissemination Identifier"
            df.rename(
                columns={
                    "Dissemintation ID": "Dissemination Identifier",
                    "Original Dissemintation ID": "Original Dissemination Identifier",
                },
                inplace=True,
            )

        master = pd.concat([master, df], ignore_index=True)

    return master


master = filter_merge()

# Treat "Original Dissemination Identifier" and "Dissemination Identifier" as long integers
master["Original Dissemination Identifier"] = master[
    "Original Dissemination Identifier"
].astype("Int64")

master["Dissemination Identifier"] = master["Dissemination Identifier"].astype("Int64")

master = master.drop(columns=["Unnamed: 0"], errors="ignore")

master.to_csv(
    r"output/filtered.csv"
)  # replace with desired path for successfully filtered and merged report

# Sort by "Event timestamp"
master = master.sort_values(by="Event timestamp")

"""
This df represents a log of all the swaps transactions that have occurred in the past two years.

Each row represents a single transaction.  Swaps are correlated by the "Dissemination ID" column.  Any records that
that have an "Original Dissemination ID" are modifications of the original swap.  The "Action Type" column indicates
whether the record is an original swap, a modification (or correction), or a termination of the swap.

We want to split up master into a single dataframe for each swap.  Each dataframe will contain the original swap and
all correlated modifications and terminations.  The dataframes will be saved as CSV files in the 'output_swaps' folder.
"""

# Create a list of unique Dissemination IDs that have an empty "Original Dissemination ID" column or is NaN
unique_ids = master[
    master["Original Dissemination Identifier"].isna()
    | (master["Original Dissemination Identifier"] == "")
]["Dissemination Identifier"].unique()


# Add unique Dissemination IDs that are in the "Original Dissemination ID" column
unique_ids = np.append(
    unique_ids,
    master["Original Dissemination Identifier"].unique(),
)


# filter out NaN from unique_ids
unique_ids = [int(x) for x in unique_ids if not np.isnan(x)]

# Remove duplicates
unique_ids = list(set(unique_ids))

# For each unique Dissemination ID, filter the master dataframe to include all records with that ID
# in the "Original Dissemination ID" column
open_swaps = pd.DataFrame()

for unique_id in tqdm(unique_ids):
    # Filter master dataframe to include all records with the unique ID in the "Dissemination ID" column
    swap = master[
        (master["Dissemination Identifier"] == unique_id)
        | (master["Original Dissemination Identifier"] == unique_id)
    ]

    # Determine if the swap was terminated.  Terminated swaps will have a row with a value of "TERM" in the "Event Type" column.
    was_terminated = (
        "TERM" in swap["Action type"].values or "ETRM" in swap["Event type"].values
    )

    if not was_terminated:
        open_swaps = pd.concat([open_swaps, swap], ignore_index=True)

    # Save the filtered dataframe as a CSV file
    output_filename = os.path.join(
        OUTPUT_PATH,
        "processed",
        f"{'CLOSED' if was_terminated else 'OPEN'}_{unique_id}.csv",
    )
    swap.to_csv(
        output_filename,
        index=False,
    )  # replace with desired path for successfully filtered and merged report

output_filename = os.path.join(
    OUTPUT_PATH, "processed", "output/processed/OPEN_SWAPS.csv"
)
open_swaps.to_csv(output_filename, index=False)
