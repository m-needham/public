# Ensemble Analysis Template

**Updated 8 September 2022**

This directory contains a template for analyzing large ensembles of climate simulations with a custom analysis function. Most of the code in this directory does not need to change for different analysis goals. Instead it is designed for the user to supply a `custom_analysis_function` to be applied identically to each ensemble member, and a `custom_save_function` to control the management of output files.

#### Contents

* Python Scripts
    * `_analysis_functions.py`
    * `_ensemble_analysis.py`
    * `_generate_casenames.py`
* Markdown Notes
    * `NOTES.md`
    * `README.md`
* Bash Submission Script
    * `submit.sh`

## Overview

The procedure is controlled by the bash script `submit.sh` which takes care of
* Submission to a PBS queue
* Loading the proper python modules
* User specification of the ensemble to analyze
* The chocie of Parallel or Serial computation
* Location to save output files

The `submit.sh` then calls two python scripts:
* `_generate_casenames.py`
    * This script essentially performs string comprehension to generate a unique casename for each member of the ensemble based on the files in the `DATA_PATH` specified in `script.sh`
* `_ensemble_analysis.py`
    * This script contains instructions for the bulk of the analysis. The procedure is:
        * Loop over ensemble members
        * Perform a calculation on each ensemble member
        * Save the results to an output `.nc` file or files
          
`_ensemble_analysis.py` imports many functions from `_analysis_functions.py` which is where most user edits should take place. 

## Typical Workflow

#### 1. Make edits to `submit.sh`

* Ensure PBS submission options are correct
    * `-N` Job name
    * `-A` Account number
    * etc.

* Specify the data frequency

```bash
# Analyze monthly mean output
ENSEMBLE_NAME="month_1"
```

> Note: this string directly corresponds to the name of one of the directories in the particular large ensemble's data path. For example, `/glade/campaign/cgd/cesm/CESM2-LE/timeseries/atm/proc/tseries/` has four directories: `day_1`  `hour_3`  `hour_6`  `month_1`

* Specify the ensemble to analyze

```bash
# Analyze the CESM2 large ensemble
ENSEMBLE_NAME="CESM2-LE"
```

> Note: the function `get_ensemble_data_path` in `_analysis_functions.py` allows the user to specify additional ensembles. However, this will require updating the file `_generate_casenames.py` to account for a different naming convention.

* Specify where output files should be stored

```bash
# Store in a custom directory in the user's work directory
SAVE_PATH="/glade/work/$USER/ensemble_analysis/"
```

> Note: If the specified directory does not exist, the script will create it.

#### 2. Make edits to `analysis_scripts.py`

* Update `setup_cluster` to request the desired programming resources for the problem and ensure the project name is correct

* Update `custom_variable_list` to include the desired variables to import and pass to the custom analysis function

* Specify `custom_analysis_function` to perform the desired computations for a single ensemble member. All of the variables specified in `custom_variable_list` will be stored in the dataset `dset_ens` for use here.

* Make necessary changes to `custom_combination_function` - the current behavior is to concatenate the dataset for each ensemble member into a single large dataset of dimensions (ensemble_member, time, ..., ...). This could also be where an ensemble mean could be calculated (consider doing this in parallel with dask.delayed!) or other secondary calculations.

* Make necessary changes to `custom_save_function` - the current behavior is to attempt to save the entire dataset from `custom_combination_function` into a single netcdf file. I have included logic here to save files for each ensemble member in case there is an error saving the one large file

#### 3. Run the script

The entire application can be run on [Casper](https://arc.ucar.edu/knowledge_base/70549550) with the command

```bash
qsub submit.sh
```

This will submit a PBS job to the casper queue. The script can instead be run on a login node (good for testing) with the command

```bash
bash submit.sh
```

although the performance of the script may appear to suffer because the PBS submission to initialize the dask cluster (see appendix below for more details about parallelism with dask) may languish in the job queue.

> It is a good idea to test that the custom analysis function works as expected. To do so, edit the `submit.sh` script to set `TESTING_MADE="TRUE"` which will apply the analysis to only two ensemble members.

## Appendix: Parallel Computation

Looping over ensemble members and performing independent calculations on each is an [embarressingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) computational task. This script is written to seamlessly take python analysis code and execute it in parallel

Parallel computation is accomplished with the python package [dask](https://docs.dask.org/en/stable/), particularly through the use of the [dask delayed](https://docs.dask.org/en/stable/delayed.html) interface. This wraps standard python functions to generate task graphs of the computation to be evaluated lazily rather than actually evaluating the computations eagerly in real-time.

The script also gives the user the opportunity to view the dask diagnostic dashboard. Log files generated by the script include instructions for viewing the dashboard for both local jobs and jobs submitted to a PBS queue.

> Currently only support for PBS systems are implemented, but systems using other job queues (e.g., SLURM) could be impemented using [dask-jobqueue](https://jobqueue.dask.org/en/latest/).