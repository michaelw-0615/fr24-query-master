# fr24-query-master
This code repo is part of the Fall 2025 CMU Heinz-American Airlines capstone project. It utilizes the API service of Flightradar24 (FR24, https://www.flightradar24.com) to query historical or live flight data, and performs essential data ETL functionalities.

## Prerequisites and dependencies
To run this repo as a project, you need:

- An FR24 API access token. This is obtained via subscription to the API service (note: not the flight tracker map subscription!).
- Python>=3.9.6

Create a virtual environment for code execution:

```powershell
python3 -m venv .<venv_name>
```

Before each run, activate the virtual environment in PowerShell:

```powershell
.source .<venv_name>/bin/activate
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
To batch-query historical flight position data, run:

```powershell
python src/batch_hist_pos_query.py
```

More functionalities to come later.

## References
For more information about the FR24 API service, see official documentation: https://fr24api.flightradar24.com/docs/
