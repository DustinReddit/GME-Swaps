This repo helps to pull down, filter, and aggregate publicly available swap data from the DTCC.

More info is available on this post:
https://www.reddit.com/r/Superstonk/comments/1e746g9/lets_demystify_the_swaps_data_do_not_trust_me_bro/

## Getting Started

1. Clone the repo
2. Set up the environment

- (Optional) Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

- Install the requirements

```bash
pip install -r requirements.txt
```

3. Run the script

```bash
python3 main.py
```

## Prerequisites

- Python 3.6+

## Output

The script will filter the data and record the transactions related to GME on a daily basis in the specified `output` folder.
The daily transactions are aggregated into a single file `filtered.csv`.

There will also be a folder named `processed` that traces all transactions swap by swap.

If the swap transactions contain an entry with an `Action type` of `TERM`, then the swap will be marked as CLOSED.
Otherwise, the swap is considered to still be OPEN.

For convenience, all open swaps are then aggregated into a single file named `OPEN_SWAPS.csv`.
