import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd
from tqdm import tqdm

from schemas import CORRELATED_CUSTOM

# Load the swaps dataset
dataset = ds.dataset("./gme_swaps", format="parquet")

# Load the swaps table
identifiers = dataset.to_table(
    columns=[
        "Dissemination Identifier",
        "Original Dissemination Identifier",
        "Action type",
    ]
)

# Add a new column to the table called "Progenitor Dissemination Identifier"
identifiers = identifiers.append_column(
    pa.field("Progenitor Dissemination Identifier", pa.int64(), nullable=True),
    pa.array([None] * identifiers.num_rows, type=pa.int64()),
)

identifiers = identifiers.to_pandas()
identifiers.drop_duplicates(inplace=True)

# We can identify the progenitors by following the chain of "Original Dissemination Identifier" values
# Since we have incomplete data, we can identify only the _true_ progenitors when there is a NEWT action.
#   In other cases, we will simply use the earliest known transaction identifier as the progenitor.

# For rows that have an action type of "NEWT" and a blank value in "Original Dissemination Identifier"
#   copy the "Dissemination Identifier" value to the "Progenitor Dissemination Identifier" column
identifiers.loc[
    (identifiers["Original Dissemination Identifier"].isna())
    & (identifiers["Action type"] == "NEWT"),
    "Progenitor Dissemination Identifier",
] = identifiers["Dissemination Identifier"]

# Now, we can find "synthetic" progenitors by looking for records that have an "Original Dissemination Identifier" value
#  that is not present in the "Dissemination Identifier" column
# Copy the "Dissemination Identifier" value to the "Progenitor Dissemination Identifier" column
#  for these synthetic progenitors
identifiers.loc[
    ~identifiers["Original Dissemination Identifier"].isin(
        identifiers["Dissemination Identifier"]
    ),
    "Progenitor Dissemination Identifier",
] = identifiers["Dissemination Identifier"]


def coalesce_progenitors(table):
    # For rows that have a value in "Original Dissemination Identifier" and a blank value in "Progenitor Dissemination Identifier"
    #  copy the "Progenitor Dissemintion Identifier" value from the row that contains the "Original Dissemination Identifier" value
    #  in the "Dissemination Identifier" column
    # For example, if the "Original Dissemination Identifier" value is 12345, and the "Dissemination Identifier" value is 67890,
    #  and the "Progenitor Dissemination Identifier" value is blank, copy the "Progenitor Dissemination Identifier" value from the row
    #  that contains the "Dissemination Identifier" value of 12345
    lacking_progenitors = table[
        (table["Progenitor Dissemination Identifier"].isna())
        & (table["Original Dissemination Identifier"].notna())
    ]

    total_lacking_count = len(lacking_progenitors)
    print(total_lacking_count)

    found_last_step = 0

    # Set up a progress bar.  We know how many total progenitors are missing, but not how many we will find in each iteration
    pbar = tqdm(
        range(total_lacking_count),
        desc="Coalescing progenitors",
        miniters=None,
        mininterval=0,
        smoothing=0.01,
        unit="rows",
    )

    while len(lacking_progenitors) > 0:
        # pbar.set_postfix(
        #     {
        #         "Total missing progenitors": total_lacking_count,
        #         "Current missing progenitors": last_lacking_count,
        #     },
        #     refresh=True,
        # )
        # Left join the table with itself on the "Dissemination Identifier" and "Original Dissemination Identifier" columns

        table = table.merge(
            table,
            how="left",
            left_on="Original Dissemination Identifier",
            right_on="Dissemination Identifier",
            suffixes=("", "_y"),
            validate="m:1",
        )

        # Select "Dissemination Identifier" and "Original Dissemination Identifier" from the left table,
        #  and "Progenitor Dissemination Identifier" from the right table if it is not null
        table["Progenitor Dissemination Identifier"] = table[
            "Progenitor Dissemination Identifier_y"
        ].combine_first(table["Progenitor Dissemination Identifier"])

        # Drop the "_y" columns
        table = table[[col for col in table.columns if not col.endswith("_y")]]

        lacking_progenitors = table[
            (table["Progenitor Dissemination Identifier"].isna())
            & (table["Original Dissemination Identifier"].notna())
        ]

        # Update the progress bar with the number of missing progenitors found in this iteration
        pbar.update(total_lacking_count - len(lacking_progenitors) - found_last_step)
        found_last_step = total_lacking_count - len(lacking_progenitors)

    pbar.close()
    return table


identifiers = coalesce_progenitors(identifiers)
print(identifiers)

# Add the "Progenitor Dissemination Identifier" column to the dataset
correlated_swaps = (
    dataset.to_table()
    .to_pandas()
    .merge(
        identifiers[
            ["Dissemination Identifier", "Progenitor Dissemination Identifier"]
        ],
        how="left",
        left_on="Dissemination Identifier",
        right_on="Dissemination Identifier",
    )
)

# Move the progenitor column to the 3rd position
correlated_swaps = correlated_swaps[
    [
        "Dissemination Identifier",
        "Original Dissemination Identifier",
        "Progenitor Dissemination Identifier",
        "Action type",
    ]
    + [col for col in correlated_swaps.columns if col not in identifiers.columns]
]

# Save a csv file with the correlated swaps
correlated_swaps.to_csv("./output/correlated_swaps.csv")

# Save the correlated swaps dataset
ds.write_dataset(
    correlated_swaps,
    base_dir="./correlated_swaps",
    format="parquet",
    schema=CORRELATED_CUSTOM,
)
