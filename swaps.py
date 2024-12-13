import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
import numpy as np
import glob
import requests
import os
import io
from zipfile import ZipFile
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
from config import (
    OUTPUT_PATH,
    PROCESSED_PATH,
    GME_SWAPS_PATH,
    MAX_WORKERS,
)
from schemas import PHASE_2, map_columns

# Define some configuration variables
GME_IDS = ["GME.N", "GME.AX", "US36467W1099", "36467W109"]

# Make paths if they don't exist
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

if not os.path.exists(PROCESSED_PATH):
    os.makedirs(PROCESSED_PATH)
else:
    # Ask the user if they want to overwrite the existing processed data
    response = input(
        "Processed data already exists. Do you want to overwrite it? (y/n): "
    )

    if response.lower() != "y":
        print("Exiting...")
        exit()

    # Remove existing processed data
    for file in glob.glob(os.path.join(PROCESSED_PATH, "*")):
        os.remove(file)

if not os.path.exists(GME_SWAPS_PATH):
    os.makedirs(GME_SWAPS_PATH)

    # Ask the user if they want to overwrite the existing GME swaps data
    response = input(
        "GME swaps data already exists. Do you want to overwrite it? (y/n): "
    )

    if response.lower() != "y":
        print("Exiting...")
        exit()

    # Remove existing GME swaps data
    for file in glob.glob(os.path.join(GME_SWAPS_PATH, "*")):
        os.remove(file)


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

def invalid_row_handler(row):
    print("Failed to parse the following row:")
    print(row)
    return "skip"


parse_options = csv.ParseOptions(invalid_row_handler=invalid_row_handler)

def retry_with_backoff(func, *args, **kwargs):
    for i in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Failed to execute {func.__name__} on try {i + 1}: {e}")
            time.sleep(2 ** i)

    print(f"Failed to execute {func.__name__} after 5 tries")
    return False

def download_and_filter(filename):
    parquet_filename = os.path.join(OUTPUT_PATH, filename.replace(".zip", ".parquet"))

    # Download the zip file if it's not present in the staging directory
    if os.path.exists(parquet_filename):
        return

    def download():
        url = f"https://pddata.dtcc.com/ppd/api/report/cumulative/sec/{filename}"
        req = requests.get(url)

        if req.status_code != 200:
            print(f"Failed to download {url}")
            return False

        return req

    req = retry_with_backoff(download)

    if not req:
        return False

    contents = io.BytesIO(req.content)

    table = None

    # Load content into dataframe
    with ZipFile(contents) as zip_ref:
        for file in zip_ref.namelist():
            in_table = csv.read_csv(zip_ref.open(file), parse_options=parse_options)

            if table is None:
                table = in_table
            else:
                table = pa.concat_tables([table, in_table])

    map_columns(table)

    writer = pq.ParquetWriter(parquet_filename, table.schema)
    writer.write_table(table)
    writer.close()

#Create working folders
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

if not os.path.exists(STAGING_PATH):
    os.makedirs(STAGING_PATH)

if not os.path.exists(PROCESSED_PATH):
    os.makedirs(PROCESSED_PATH)
else:
    files = glob.glob(PROCESSED_PATH + '/*.parquet')
    for f in files:
        os.remove(f)

tasks = []
for filename in filenames:
    tasks.append(executor.submit(download_and_filter, filename))

for task in tqdm(as_completed(tasks), total=len(tasks)):
    try:
        task.result()
    except Exception as e:
        # Clean up tasks and exit
        for task in tasks:
            task.cancel()

        raise e

# # Load all parquet files into a single dataset
dataset = ds.dataset(
    OUTPUT_PATH,
    format="parquet",
    schema=PHASE_2,
)

# # Merge / Split into files aligned with the row group size
ds.write_dataset(
    dataset,
    base_dir=PROCESSED_PATH,
    format="parquet",
    min_rows_per_group=500000,
    max_rows_per_file=5 * 10**6,
)

dataset = ds.dataset(
    PROCESSED_PATH,
    format="parquet",
    schema=PHASE_2,
)

print("Locating swaps containing GME...")

ids = []
for batch in tqdm(
    dataset.to_batches(columns=["Dissemination Identifier", "Underlier ID-Leg 1"])
):
    masks = [
        pc.match_substring(batch.column("Underlier ID-Leg 1"), gme_id)
        for gme_id in GME_IDS
    ]

    m = masks[0]
    for mask in masks[1:]:
        m = pc.or_(m, mask)

    matches = batch.filter(m)
    ids.extend(matches.column("Dissemination Identifier").to_pylist())

gme_swaps = dataset.to_table(filter=ds.field("Dissemination Identifier").isin(ids))

print("Collecting Identifiers...")
identifier_projection = {
    "Dissemination Identifier": ds.field("Dissemination Identifier"),
    "Original Dissemination Identifier": ds.field("Original Dissemination Identifier"),
}


# Identify "Original Dissemination Identifier" values that are not present in the "Dissemination Identifier" column
print("Identifying orphaned swaps...")


def find_orphans(table):
    mask = pc.invert(
        pc.is_in(
            table.column("Original Dissemination Identifier"),
            table.column("Dissemination Identifier"),
            skip_nulls=True,
        )
    )

    orphaned_ids = table.filter(mask).filter(
        ~ds.field("Original Dissemination Identifier").is_nan()
    )

    return orphaned_ids


def find_parents(table, dataset):
    identifiers = dataset.to_table(columns=identifier_projection)
    orphans = find_orphans(table)

    parent_ids = []

    last_orphan_count = orphans.num_rows

    pbar = tqdm(total=last_orphan_count)

    while orphans.num_rows > 0:
        orphaned_ids = pc.unique(orphans.column("Original Dissemination Identifier"))

        # Parent IDs will always be smaller than the orphaned IDs, so we can filter out the identifiers
        # with an ID that is larger than the largest orphaned ID
        largest_orphaned_id = pc.max(orphaned_ids).as_py()
        identifiers = identifiers.filter(
            ds.field("Dissemination Identifier") <= largest_orphaned_id
        )

        parents = identifiers.filter(
            ds.field("Dissemination Identifier").isin(orphaned_ids)
        )


        if parents.num_rows == 0:
            print("No parents found")
            break

        parent_ids.extend(parents.column("Dissemination Identifier").to_pylist())

        # print(f"Found {parents.num_rows} parents for {orphans.num_rows} orphans...")

        orphans = find_orphans(parents)
        pbar.update(parents.num_rows)
        last_orphan_count = orphans.num_rows

    if len(parent_ids) == 0:
        return table

    all_parents = dataset.to_table(
        filter=ds.field("Dissemination Identifier").isin(parent_ids)
    )

    table = pa.concat_tables([table, all_parents])
    pbar.close()
    return table


all_gme_swaps = find_parents(gme_swaps, dataset)

# Save all GME swaps to a new dataset
ds.write_dataset(
    all_gme_swaps,
    base_dir="./gme_swaps",
    format="parquet",
    min_rows_per_group=500000,
    max_rows_per_file=5 * 10**6,
)
