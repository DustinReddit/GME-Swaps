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

3. Config

You can change any of the configuration by modifying the `config.py` file.

4. Run the scripts

```bash
python3 swaps.py
python3 correlate_swaps.py
```

## Prerequisites

- Python 3.6+

## Output

The script will filter the data and record the transactions related to GME on a daily basis in the specified `output` folder.
There will also be a folder named `processed` that traces all transactions swap by swap.

After running `correlate_swaps.py` there is a file named `correlated_swaps.csv` that is all the swaps data correlated by "Progenitor ID".
There is also a parquet dataset that is created in the `output` folder that contains all the swaps data for easier querying.
