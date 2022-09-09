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

# ==============================================================================
# FUNCTION: Parse command line arguments
# ==============================================================================

def parse_command_line_arguments():
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--casenames_file',type=str)
    parser.add_argument('--data_freq',type=str)
    parser.add_argument('--ensemble_name',type=str)
    parser.add_argument('--job_scheduler',type=str)
    parser.add_argument('--parallel',type=str,default="TRUE")
    parser.add_argument('--save_path',type=str)
    parser.add_argument('--save_name',type=str)
    parser.add_argument('--testing_mode',type=str,default="FALSE")
    parser.add_argument("--user",type=str)
    parser.add_argument("--verbose", nargs='?', type=int, const=10, default=20)

    args      = parser.parse_args()
    
    return args

# ==============================================================================
# FUNCTION: Get supported ensembles
# ==============================================================================

def get_supported_ensembles():
    supported_ensembles = [
        "CESM2-SF",
        "CESM2-LE",
    ]
    
    return supported_ensembles


# ==============================================================================
# FUNCTION: Get ensemble data path
# ==============================================================================

def get_ensemble_data_path(ensemble_name):

    ensemble_paths = {
        "CESM2-SF":"/glade/campaign/cesm/collections/CESM2-SF/timeseries/atm/proc/tseries/",
        "CESM2-LE":"/glade/campaign/cgd/cesm/CESM2-LE/timeseries/atm/proc/tseries/",
    }
    
    return ensemble_paths[ensemble_name]


# ==============================================================================
# FUNCTION: Read Casenames from Text File
# ==============================================================================

def read_casenames(casenames_file):
    
    logging.info("Reading in Case Names")
    
    casenames = []
    with open(casenames_file,mode='r') as file:
        for line in file.readlines():
            casenames.append(line[:-1])
            
    return casenames

# ==============================================================================
# FUNCTION: Generate filenames for each ensemble member
# ==============================================================================

def generate_ensemble_filenames(netcdf_variables,casenames,path,delimeter="&&"):
    
    logging.info("Generating lists of files for each ensemble member")
    
    # Empty dict to hold all files for all ensemble members    
    case_files = {} 

    # Only used for debugging
    ncases = len(casenames)
    i = 1

    # Generate a list of filenames for each ensemble member
    for CASENAME in casenames:
        
        logging.debug(f"Generating Files for Case {i} of {ncases}: {CASENAME}")

        # Initialize an empty list
        CASEFILES_LIST = []    
        
        if delimeter in CASENAME:
            CASENAME_LIST = CASENAME.split(delimeter)
            
        else:
            CASENAME_LIST = [CASENAME]

        # Iterate over desired variables: this is because data is stored in 
        # individual timeseries files for each variable
        for var in netcdf_variables:
            
            # Temporary file directory for a given variable. Note that the file directory
            # holds files for every ensemble member
            file_dir_tmp = path + var + "/"

            # Get all files for a given variable
            files_tmp = [file_dir_tmp + x for x in os.listdir(file_dir_tmp) if ".nc" in x]
            
            # -----------------------------------------------------------------
            # START: Treatment for same-case, different-name 
            # (e.g., a single run with historical forcing and then a SSP
            # forcing, where the historical / ssp data have slightly different
            # filenames that prevent simple string comprehension)
            # -----------------------------------------------------------------            
            files_tmp_case = []
            
            for CASE in CASENAME_LIST:
                
                # Filter to only include files for the specified ensemble member
                files_tmp_case = files_tmp_case + [str(x) for x in files_tmp if CASE in x]
                        
            # Add files for the given variable to list of all files for the specified
            # ensemble member
            CASEFILES_LIST = np.sort(np.append(CASEFILES_LIST,files_tmp_case))
            
        # Store the list of files for a specific ensemble member in the dict
        case_files[CASENAME] = CASEFILES_LIST
        
        i += 1
            
    return case_files

def setup_cluster(user,job_scheduler="PBS"):
    
    if job_scheduler == "PBS":
        
        logging.info("Setting up a dask PBSCluster")
    
        # Setup your PBSCluster - make sure that it uses the casper queue
        cluster = PBSCluster(
                cores=1, # The number of cores you want
                memory='128GB', # Amount of memory
                processes=12, # How many processes
                queue='casper', # The type of queue to utilize (/glade/u/apps/dav/opt/usr/bin/execcasper)
                local_directory='$TMPDIR', # Use your local directory
                resource_spec='select=1:ncpus=6:mem=128GB', # Specify resources
                project='UHAR0008', # Input your project ID here
                walltime='02:00:00', # Amount of wall time
                interface='ib0', # Interface to use
            ) 
        
    # elif job_scheduler == "SLURM":
        
        # logging.info("Setting up a dask SLURMCluster)
        # use dask-jobqueue.SLURMCluster to set up a cluster       
        
    else:
        
        cluster = "FAILED"
        client  = "FAILED"
        
        job_scheduler_not_recognized_message = f'''
=======================================================================================================================
!!!UNABLE TO SETUP DASK CLUSTER!!!
=======================================================================================================================
THE JOB_SCHEDULER SUPPLIED BY THE USER: 
        {job_scheduler}
        
IS NOT CURRENTLY SUPPORTED BY THIS APPLICATION.
THIS CAN BE REMIDIED BY ADDING AN OPTION TO 
        setup_cluster() IN THE SCRIPT _analysis_functions.py
        
EXITING
        '''
        
        logging.error(job_scheduler_not_recognized_message)
        
        return cluster, client 
        
    cluster.scale(10)

    client = Client(cluster)

    # This is just to forward the host for ssh tunnelling so the dask dashboard
    # can be viewed remotely.
    host = client.run_on_scheduler(socket.gethostname)
        
    dask_logging_text = f'''    
=======================================================================================================================
DASK DIAGNOSTICS
=======================================================================================================================
To view the dask diagnostic dashboard for this job, follow this URL for a local or login-node job:
        {client.dashboard_link}
        
If this is a remote PBS job on a HPC, need to first use SSH tunnelling. Login to cheyenne with the following command:
        ssh -N -L 8888:{host}:8888  -L 8787:{host}:8787 {user}@casper.ucar.edu
Then use this url:
        http://localhost:8787/status
        
        '''

    logging.info(dask_logging_text)           
    
    return cluster, client

# ==============================================================================
# ==============================================================================
# CUSTOM USER FUNCTIONS
# ==============================================================================
# ==============================================================================

# ==============================================================================
# FUNCTION: custom variable list
# 
# Pass the list of variables for import 
# ==============================================================================


def custom_variable_list():
    
    logging.info("Getting list of variables for import")
    
    netcdf_variables = [
        "FLNT",
        "FLNTC",
        "FSNT", 
        "FSNTC",
        # Add variables here
    ]
    
    for var in netcdf_variables:
        logging.debug(f" * {var}")
    
    return netcdf_variables

# ==============================================================================
# FUNCTION: custom analysis function
# 
# Perform the primary analysis for a single ensemble member
# ==============================================================================

def custom_anaylsis_function(dset_ens, case_name):
    logging.debug(f'Performing analysis for ensemble member: {case_name}')
    
    logging.debug(f'Calculating cloud radiative effect at TOA')
    
    # Calculate CRE from full-sky and clear-sky data
    lwcre = dset_ens.FLNT - dset_ens.FLNTC
    swcre = dset_ens.FSNT - dset_ens.FSNTC
    
    # Add metadata
    lwcre = lwcre.assign_attrs(
        {
            "long_name":"longwave cloud radiative effect at TOA",
            "units":"W/m2"
        }
    )
    swcre = swcre.assign_attrs(
        {
            "long_name":"shortwave cloud radiative effect at TOA",
            "units":"W/m2"
        }
    )
    
    logging.debug(f'Adding to dataset for ensemble member: {case_name}')
    
    dset_ens["LWCRE"] = lwcre
    dset_ens["SWCRE"] = swcre    
    
    output = dset_ens
    
    return output # output should be an xr dataset

# ==============================================================================
# FUNCTION: custom combination function
# 
# Combine output from every ensemble member into a desired format
# ==============================================================================


def custom_combination_function(analysis_output_computed):
    
    logging.info(f'Combining data...')
    
    # Combine all of the analysis output into a single xr dataset along a new
    # dimension of "ensemble member"
    dset_save = xr.concat([x for x in analysis_output_computed.values()],dim='ensemble_member')
    
    # Give appropriate names to the ensemble members, rather than just indexes
    dset_save = dset_save.assign_coords({'ensemble_member':list(analysis_output_computed.keys())})
    
    return dset_save

# ==============================================================================
# FUNCTION: custom save function
# 
# Capability to save the data in one or multiple formats 
# (e.g., maps and zonal means from data, or one file for each variable)
#
# I have included logic here to ensure that data is not lost after calculation
# just because of an issue with writing all ensemble members to a single
# netcdf file
# ==============================================================================


def custom_save_function(dset_save,save_path,save_name,parallel,ensemble_name,data_path):
    
    logging.info(f'Saving data...')
    
    # Create the save directory if it does not exist
    if not os.path.exists(save_path):
        logging.info(f'Creating directory {save_path}')
        os.mkdir(save_path)
    
    # String manipulations to generate appropriate path/filename
    save_str      = f"_{len(dset_save.ensemble_member)}_ens_members"
    save_filename = f"{ENSEMBLE_NAME}_{save_name}"
    SAVE_NAME     = save_path + save_filename + save_str + ".nc"
    
    # serialization issue -> remove variables date_written, time_written
    problem_vars = ["date_written","time_written"]
        
    for var in problem_vars:
        
        try:
            dset_save = dset_save.drop_vars(var)
            logging.debug(f"Removed {var} from dataset")
        except ValueError:
            logging.debug(f"{var} not found in dataset")
    
    try:
        
        logging.info("Attempting to write all ensemble members to the same file.")
        
        # variable "time_encoding" largely copied from the original netcdf files
        # Not sure why I need to specify the netcdf ncoding, but adding this step
        # supressed some warnings and it doesn't appear to break anything else
        time_encoding = {
            'zlib': True, 
            'shuffle': True, 
            'complevel': 1, 
            'fletcher32': False, 
            'contiguous': False, 
            'chunksizes': (512,), 
            'source': data_path, 
            'original_shape': (600,), 
            'dtype': np.dtype('float64'), 
            'units': dset_save.time.units, 
            'calendar': 'noleap'
        }

        encoding = {'time':time_encoding}
        
        if parallel == "TRUE":
            
            logging.info("Writing files in parallel")
        
            dset_save.to_netcdf(SAVE_NAME,encoding=encoding,compute=False)
            
            dset_save.compute()
            
        else:
            
            dset_save.to_netcdf(SAVE_NAME,encoding=encoding,compute=True)
        
        logging.info(f'Data successfully saved to:\n    {SAVE_NAME}')  
        
    except:
        
        # Now that we are writing one file for each ensemble member,
        # create a new directory in the old one to hold this data
        new_save_path = save_path + save_name + "/"     
        
        # Only attempt to make the dir if it does not already exist
        if not os.path.exists(new_save_path):
            os.mkdir(new_save_path)        
        
        # Remove the file that the script unsuccessfully attempted to write
        if not os.path.exists(SAVE_NAME):
            os.remove(SAVE_NAME)
        
        logging.warning("UNABLE TO WRITE ALL ENSEMBLE MEMBERS TO THE SAME FILE")
        logging.warning("ATTEMPTING TO WRITE A ONE FILE FOR EACH ENSEMBLE MEMBER")
        logging.warning("SAVING FILES IN A NEW DIRECTORY:")
        logging.warning(f"    {new_save_path}")
        
        # keep track of any cases script is unable to save
        problem_cases = []
        
        ncases   = len(dset_save.ensemble_member) 
        i = 1
        
        for ENS_NAME in dset_save.ensemble_member:
            
            try:
                
                # String manipulations to generate appropriate path/filename for 
                # each ensemble member
                ENS_NAME_STR  = str(ENS_NAME.data)
                NEW_SAVE_NAME = new_save_path + ENS_NAME_STR + save_filename + ".nc"
            
                logging.info(f"Case {i} of {ncases}. Saving {ENS_NAME_STR + save_filename + '.nc'}")
                i += 1

                # Save the data for the single ensemble member
                
                if parallel == "TRUE":
                    dset_save.sel(ensemble_member=ENS_NAME).to_netcdf(NEW_SAVE_NAME,encoding=encoding,compute=False)
                    
                    dset_save.compute()
                    
                else:
                    
                    dset_save.sel(ensemble_member=ENS_NAME).to_netcdf(NEW_SAVE_NAME,encoding=encoding,compute=True)
                
            except:
                
                # Remove the file that the script unsuccessfully attempted to write
                if not os.path.exists(NEW_SAVE_NAME):
                    os.remove(NEW_SAVE_NAME)
                
                logging.error(f"UNABLE TO SAVE DATA FOR CASE {ENS_NAME_STR}")    
                
                problem_cases.append(ENS_NAME_STR)
                
        # If there were any problem cases, list in log for user
        if problem_cases != []:
            logging.error("UNABLE TO SAVE DATA FOR THE FOLLOWING CASES:")
            for case in problem_cases:
                logging.error(case)
                
            
            nerror   = len(problem_cases)
            nsuccess = ncases - nerror
            logging.error(f"SUCCESSFULLY WROTE {nsuccess}/{ncases} FILES")
                
        else:
            logging.warning("ALL CASES SUCCESSFULLY SAVED TO INDIVIDUAL FILES")
            logging.info(f"Saved data located:\n    {new_save_path}")
            
    return