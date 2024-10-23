# Kpler Pipelines and e2e tests

![develop branch status](https://github.com/glencore-gbldn/glencore-synergy-pipelines-kpler/actions/workflows/run-all-tests.yml/badge.svg)

## Running a specific set of e2e tests

It is rare that you will need to run ALL of the e2e tests in this repo to check that a code change to a given notebook. To run specific tests, you should call the workflow dispatcher on the Actions tab and provide a combination of tags that will define which set of tests to run. 

The tags have been picked to reflect the naming conventions used by the notebooks. For example the e2e test for the `kpler_certified_lpg_stem` notebook can be run using the tags `kpler and certified and lpg and stem` (in fact, as all the tests in this codebase are for kpler, you don't even need the `kpler` tag).

If you want to run all the LPG tests, then just say `lpg`. Or if you want to run all the LPG stem tests, then it's `lpg and stem`.

### Tags for notebooks in the `kpler` folder

The following tags are used in combination for the tests that relate to the `kpler` notebooks:

- raw
- curated
- certified
- cleaned

- lng
- lpg
- oil
- vessels_lng

- intermediary_link
- ship_to_ship
- stem
- trade
- tracking_intransit
- vessels

### Tags for notebooks in the `cosmos_db_export` folder

- cosmos_db_export
- sts
- trade_stem

## Running all the tests

Running all the tests at once can expose limitations of the infrastructure - you may end up with jobs just failing with no more explanation than the "job failed" message. If you want to run all the tests, be aware of this and don't be surprised if you have to rerun some of the jobs.

To run all the tests, just provide `kpler` as the value for the test tags, or leave it empty.


## Naming conventions for feature files

Where possible (and it usually is possible), feature files should be named after the notebook they test. For example, if the notebook is in `notebooks\cosmos_db_export\sts_cosmos_db_export.py`, then the corresponding feature file should be `features\cosmos_db_export\sts_cosmos_db_export.feature`.

## Notebooks that don't have a corresponding e2e test

The following notebooks don't currently have e2e tests - you won't find feature files for them.

- `kpler\lng\kpler_cleaned_lng_tracking_intransit.py`
- `kpler\vessels_lng\kpler_cleaned_vessels_lng.py`

There are currently no e2e tests for any of the data ingestion pipelines.