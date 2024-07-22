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

from schemas import PHASE_2, map_columns

# Define some configuration variables
OUTPUT_PATH = r"./output"  # path to folder where you want filtered reports to save
STAGING_PATH = (
    r"./staging"  # path to folder where you want to download and extract reports
)
PROCESSED_PATH = (
    r"./processed"  # path to folder where you want processed reports to save
)
MAX_WORKERS = 24  # number of threads to use for downloading and filtering

GME_IDS = ["GME.N", "GME.AX", "US36467W1099", "36467W109"]

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Generate daily dates from two years ago to today
start = datetime.datetime.today() - datetime.timedelta(days=730)
end = datetime.datetime.today()
dates = [start + datetime.timedelta(days=i) for i in range((end - start).days + 1)]

# Generate dates for the first day of the month for the past 24 months
# months = [(datetime.datetime.today().month - 1 + i) % 12 + 1 for i in range(24)]
# years = [
#     datetime.datetime.today().year - ((i + datetime.datetime.today().month - 2) // 12)
#     for i in range(24, 0, -1)
# ]
# dates = [datetime.datetime(year, month, 1) for year, month in zip(years, months)]

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


def download_and_filter(filename):
    parquet_filename = os.path.join(OUTPUT_PATH, filename.replace(".zip", ".parquet"))

    # Download the zip file if it's not present in the staging directory
    if os.path.exists(parquet_filename):
        return

    url = f"https://pddata.dtcc.com/ppd/api/report/cumulative/sec/{filename}"
    req = requests.get(url)

    if req.status_code != 200:
        print(f"Failed to download {url}")
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

identifiers = dataset.to_table(columns=identifier_projection)

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
    orphans = find_orphans(table)

    parent_ids = []
    while orphans.num_rows > 0:
        orphaned_ids = pc.unique(orphans.column("Original Dissemination Identifier"))

        parents = identifiers.filter(
            ds.field("Dissemination Identifier").isin(orphaned_ids)
        )

        if parents.num_rows == 0:
            print("No parents found")
            break

        parent_ids.extend(parents.column("Dissemination Identifier").to_pylist())

        print(f"Found {parents.num_rows} parents for {orphans.num_rows} orphans...")

        orphans = find_orphans(parents)

    if len(parent_ids) == 0:
        return table

    all_parents = dataset.to_table(
        filter=ds.field("Dissemination Identifier").isin(parent_ids)
    )

    table = pa.concat_tables([table, all_parents])
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
