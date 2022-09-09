# ==============================================================================
# Import Statements
# ==============================================================================

import argparse
import dask
import logging
import os
import numpy  as np
import socket
import time
import datetime
import xarray as xr 

from dask.distributed import Client
from dask_jobqueue import PBSCluster

from _analysis_functions import *

# ==============================================================================
# Main Function Call
# ==============================================================================
def main():
    
    # ==========================================================================
    # Section 1
    # ==========================================================================
    #    * 1.A Parse command line argument
    #    * 1.B Initialize Logging
    #    * 1.C Setup Parallel / Serial Analysis
    #    * 1.D Read in list of case names from file
    # ==========================================================================
    
    start_time = datetime.datetime.now()
    
    # --------------------------------------------------------------------------
    # 1.A Parse command line argument
    # -------------------------------------------------------------------------- 
    
    args           = parse_command_line_arguments()

    CASENAMES_FILE = args.casenames_file
    DATA_FREQ      = args.data_freq
    ENSEMBLE_NAME  = args.ensemble_name.upper()
    JOB_SCHEDULER  = args.job_scheduler.upper()
    PARALLEL       = args.parallel.upper()
    SAVE_PATH      = args.save_path
    SAVE_NAME      = args.save_name
    TESTING_MODE   = args.testing_mode.upper()
    VERBOSE        = args.verbose 
    USER           = args.user  
    
    # --------------------------------------------------------------------------
    # 1.B Initialize Logging
    # --------------------------------------------------------------------------    

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        encoding='utf-8',
        level=VERBOSE, # default level is 20 (info)
        datefmt='%Y-%m-%d %H:%M:%S',        
    )

    logging.info(f'Logging initialized at level {VERBOSE}.')
    
    # --------------------------------------------------------------------------
    # 1.C Setup Parallel / Serial Analysis
    # --------------------------------------------------------------------------
    if PARALLEL == "FALSE":
        
        logging.info(f"Flag \"parallel\" set to FALSE. Computation Proceeding in Serial")
        
        parallel_or_serial_open_function     = xr.open_mfdataset
        parallel_or_serial_analysis_function = custom_anaylsis_function
    
    elif PARALLEL == "TRUE":
        
        logging.info(f"Flag \"parallel\" set to TRUE.")
        
        parallel_or_serial_open_function     = dask.delayed(xr.open_mfdataset)
        parallel_or_serial_analysis_function = dask.delayed(custom_anaylsis_function)
        
        logging.info(f'Initializing dask client')
        
        cluster, client = setup_cluster(user=USER,job_scheduler=JOB_SCHEDULER)
        
        if type(cluster) == str:
            return
        
    else:
        
        logging.error(f"UNABLE TO INTERPRET FLAG PARALLEL = \"{args.parallel}\"")
        logging.error(f"PARALLEL MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error(f"EXITING")
        
        return

    # --------------------------------------------------------------------------
    # 1.D Read in list of case names from file
    # -------------------------------------------------------------------------- 
            
    CASENAMES = read_casenames(casenames_file=CASENAMES_FILE)
    
    if TESTING_MODE == "TRUE":
        
        n_ensembles_for_test = 2
    
        testing_logging_text = f'''

======================================================================================================================
RUNNING SCRIPT IN TESTING MODE
======================================================================================================================
ANALYZING {n_ensembles_for_test} ENSEMBLE MEMBERS
            '''

        logging.warning(testing_logging_text)

        CASENAMES = CASENAMES[:n_ensembles_for_test]
        
    elif TESTING_MODE == "FALSE":
        
        # don't need to do anything
        pass
    
    else:
        
        logging.error(f"UNABLE TO INTERPRET FLAG TESTING_MODE = \"{args.testing_mode}\"")
        logging.error(f"TESTING_MODE MUST BE EITHER \"TRUE\" OR \"FALSE\"")
        logging.error(f"EXITING")
        
    ncases = len(CASENAMES)
    
    # ==========================================================================
    # Section 1 - COMPLETE
    # ==========================================================================
    
    # ==========================================================================
    # Section 2
    # ==========================================================================
    #    * 2.A Generate list of filenames for each ensemble member
    #    * 2.B Iterate over ensemble members
    #        * 2.B.1 Prepare analysis
    #        * 2.B.2 Collect results
    #    * 2.C
    #       * PARALLEL: Perform delayed computation
    #       * SERIAL: No Action
    #    * 2.D Combine results
    #    * 2.E Save data to disk
    # ==========================================================================    

    # --------------------------------------------------------------------------
    # 2.A Generate list of filenames for each ensemble member
    # --------------------------------------------------------------------------  

    # Get list of variables to load
    NETCDF_VARIABLES = custom_variable_list()
    
    DATA_PATH = get_ensemble_data_path(ENSEMBLE_NAME) + DATA_FREQ + "/"
    
    CASE_FILES = generate_ensemble_filenames(
        netcdf_variables = NETCDF_VARIABLES,
        casenames        = CASENAMES,
        path             = DATA_PATH
    )  
    
    # --------------------------------------------------------------------------
    # 2.B Iterate over ensemble members
    # --------------------------------------------------------------------------  
    
    logging.info(f'Iterating over ensemble members')
    
    # Empty dict to hold analysis output for every ensemble member
    ANALYSIS_OUTPUT_LIST = {}
    
    for ENS_MEMBER in CASENAMES:
        
        # --------------------------------------------------------------------------
        # 2.B.1 Prepare Parallel or Serial Analysis
        # --------------------------------------------------------------------------          
        
        logging.debug(f'Prepare task graph for Case: {ENS_MEMBER}')
        
        # Get the files for the particular ensemble member
        ens_member_files = CASE_FILES[ENS_MEMBER]
        
        # Need to specify as 9 or below to log all files
        if int(VERBOSE) < 10:
            for file in ens_member_files:
                logging.debug(file)
        
        # read the data - use dask delayed
        dset_ens = parallel_or_serial_open_function(ens_member_files, combine='by_coords')
        
        # Create the task graph of the custom analysis function for lazy eval
        analysis_output = parallel_or_serial_analysis_function(dset_ens,ENS_MEMBER)
        
        # --------------------------------------------------------------------------
        # 2.B.2 Collect results (delayed objects)
        # --------------------------------------------------------------------------  
        
        # Store the delayed objects in a dict for later computation
        ANALYSIS_OUTPUT_LIST[ENS_MEMBER] = analysis_output
        
    logging.info('COMPLETED Iterating over ensemble members.')
        
    # --------------------------------------------------------------------------
    # 2.C.PARALLEL Perform delayed computation and collect results
    # --------------------------------------------------------------------------
    
    if PARALLEL == "TRUE":
           
        logging.info(f'Performing delayed parallel computation. Note, a long wait here may indicate the PBS job to initialize the cluster is waiting in the job queue.')

        ANALYSIS_OUTPUT_COMPUTED_LIST = dask.compute([x for x in ANALYSIS_OUTPUT_LIST.values()])[0]
        
    # --------------------------------------------------------------------------
    # 2.C.SERIAL Collect Results
    # --------------------------------------------------------------------------        
        
    else:
        
        # Take the values of the computation as a list
        ANALYSIS_OUTPUT_COMPUTED_LIST = [x for x in ANALYSIS_OUTPUT_LIST.values()]
    
    # --------------------------------------------------------------------------
    # 2.D Combine results
    # --------------------------------------------------------------------------  
    
    logging.info('Computations complete. Preparing to combine output for saving')
  
    # Combine the casenames and computed output into a dictionary
    
    logging.debug("="*120)
    logging.debug(f"ANALYSIS_OUTPUT_COMPUTED_LIST")
    for x in ANALYSIS_OUTPUT_COMPUTED_LIST:
        logging.debug(x)
    logging.debug("="*120)
    
    ANALYSIS_OUTPUT_COMPUTED = dict(zip(CASENAMES, ANALYSIS_OUTPUT_COMPUTED_LIST))
    
    logging.info(f'Combining output for saving')
    
    dset_save = custom_combination_function(ANALYSIS_OUTPUT_COMPUTED)
    
    # --------------------------------------------------------------------------
    # 2.e Save data to disk
    # --------------------------------------------------------------------------  

    custom_save_function(dset_save,SAVE_PATH,SAVE_NAME,PARALLEL,ENSEMBLE_NAME,DATA_PATH)
    
    end_time = datetime.datetime.now()
    
    time_delta = end_time - start_time
    
    logging.info('Analysis script complete.')
    
    logging.info(f'Analysis script duration:    {time_delta}')
    
    if PARALLEL == "TRUE":
        
        logging.info("Closing cluster...")
        
        try:
            cluster.close()
            client.shutdown()
    
        # Ignore an error associated with shutting down the cluster
        except AssertionError:
            pass
    
    # ==========================================================================
    # Section 2 - COMPLETE
    # ==========================================================================
        
if __name__ == "__main__":
    main()