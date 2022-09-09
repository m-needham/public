#!/bin/bash -l
### NAME OF JOB FOR THE QUEUE
#PBS -N ens_analysis               

### ACCOUNT NUMBER
#PBS -A PROJECT                    

### SPECIFY COMPUTING RESOURCES 
### NOTE: Only request a small amount here because
### the majority of the computing is done on a separate
### Dask cluster that is initialized by the script
#PBS -l select=1:ncpus=1:mem=128GB   

### SPECIFIY JOB MAX WALLTIME
#PBS -l walltime=02:00:00           

### USE CASPER JOB QUEUE
#PBS -q casper     

# JOIN OUTPUT AND ERROR STREAMS INTO A SINGLE FILE 
#PBS -j oe                          
#------------------------------------------------------------------------------

echo "Beginning ensemble analysis script"

export TMPDIR=/glade/scratch/$USER/temp
mkdir -p $TMPDIR

# Ensure the proper python environment is active
module del python
module load conda/latest
conda activate py_ucar

# -----GLOBAL VARIABLES FOR ALL SCRIPTS----------------------------------------

# CASENAMES_FILE:  Name of local text file to hold casenames
# DATA_FREQ:       Time frequency for input data (see README for details)
# ENSEMBLE_NAME:   String identifier to help with functions. See _analysis_functions.py for a list of supported members
# JOB_SCHEDULER:    Type of system for the dask cluster
# PARALLEL:        (valid: "TRUE", "FALSE") Use Parallel or Serial computing 
# SAVE_PATH:       Location to store output files
# SAVE_NAME:       String identifier for output files
# TESTING_MODE:    (valid: "TRUE", "FALSE") If "TRUE", perform analysis on only two ensemble members
# VERBOSE:         Output level for log file (10 - debug, 20 - info, 30 - warning, 40 - error)

CASENAMES_FILE="casenames.txt"
DATA_FREQ="month_1"
ENSEMBLE_NAME="CESM2-LE"
JOB_SCHEDULER="SLURM"
PARALLEL="TRUE"
SAVE_PATH="/glade/work/$USER/data_misc/cesm2_lens/cloud_radiative_effect/"
SAVE_NAME="cld-rad-effect-toa" 
TESTING_MODE="TRUE"
VERBOSE="20" 

# -----PERFORM ANALYSIS WITH PYTHON SCRIPTS------------------------------------

# 1. GENERATE A LIST OF CASENAMES FROM THE SPECIFIED ENSEMBLE
python3 _generate_casenames.py --casenames_file $CASENAMES_FILE --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME

# 2. PERFORM THE PRIMARY DATA ANALYSIS
python3 _ensemble_analysis.py --casenames_file $CASENAMES_FILE --data_freq $DATA_FREQ --ensemble_name $ENSEMBLE_NAME --job_scheduler $JOB_SCHEDULER --parallel $PARALLEL --save_path $SAVE_PATH --save_name $SAVE_NAME --testing_mode $TESTING_MODE --user $USER --verbose $VERBOSE 

echo "Finished ensemble analysis script"

