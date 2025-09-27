# fr24-query-master
This code repo is part of the Fall 2025 CMU Heinz-American Airlines capstone project. It utilizes the API service of Flightradar24 (FR24, https://www.flightradar24.com) and the Transtats database by USDoT (https://transtats.bts.gov) to query historical or live flight data, and performs essential data ETL functionalities.

## Prerequisites and dependencies
To run this repo as a project, you need:

- An FR24 API access token. This is obtained via subscription to the API service (note: not the flight tracker map subscription!).
- Python>=3.9.6
- Manually download the Reporting Carrier On-Time Performance (https://transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ) and T-100 Domestic Segments (https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FIM) data tables, and put these into the ./inputs subfolder, if running the dot_flight_data_query or dot_flight_data_merge scripts.

Create a virtual environment for code execution:

```powershell
python3 -m venv .<venv_name>
```

Before each run, activate the virtual environment in PowerShell:

```powershell
source .<venv_name>/bin/activate
```

Within the virtual environment, use pip to download the following dependencies:
```powershell
pip install numpy pandas requests fr24sdk
```

After running, deactivate the virtual environment:

```powershell
deactivate
```

## Run
To batch-query historical flight position data using FR24 API, run:

```powershell
python src/batch_hist_pos_query.py
```

To merge T-100 tables from different years into one, run:

```powershell
python3 src/dot_t100_flight_data_merge.py \\
--inputs inputs/US_CARRIER_SUMMARY_2023.csv inputs/US_CARRIER_SUMMARY_2024.csv \\
--out outputs/US_AA_10airports.csv \\
--filter-aa \\
--project-minimal \\
--dedupe YEAR,MONTH,ORIGIN,DEST,UNIQUE_CARRIER \\
--aircraft-types inputs/DOT_AIRCRAFT_TYPE.csv && ls -l outputs/US_AA_10airports.csv && wc -l outputs/US_AA_10airports.csv && head -n 10 outputs/US_AA_10airports.csv
```

To merge different DoT Transtats tables into one final output that contains all flight records with aircraft types, run:

```powershell
python3 src/dot_final_merge.py \\
--aa_test inputs/aa_flight_test.csv \\
--merged outputs/US_AA_10airports.csv \\
--out outputs/aa_flight_test_enriched.csv \\
--aircraft-types inputs/DOT_AIRCRAFT_TYPE.csv && wc -l \\
outputs/aa_flight_test_enriched.csv && head -n 10 \\
outputs/aa_flight_test_enriched.csv
```

Change file names if necessary.

## References
For more information about the FR24 API service, see official documentation: https://fr24api.flightradar24.com/docs/

For more information about the Transtats database, see: https://www.transtats.bts.gov/DataIndex.asp
