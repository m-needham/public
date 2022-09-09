# ==============================================================================
# Import Statements
# ==============================================================================

import argparse
import logging
import os
import numpy  as np

from _analysis_functions import *

# ==============================================================================
# Generate Case Names
# ==============================================================================

def generate_case_names(path):

    # List of all files in path
    files = np.sort([x for x in os.listdir(path) if ".nc" in x])

    # Split the string on ".cam."
    cases = []

    for file in files:
        
        # get the central string between "f09_g17" and ".cam."
        file_tmp = file.split("f09_g17.")[1]
        file_tmp = file_tmp.split(".cam.")[0]
        
        cases.append(file_tmp)
            
    cases = np.unique(cases)

    return cases

def combine_split_cases(cases,ENSEMBLE_NAME,delimiter="&&"):
    
    if ENSEMBLE_NAME == "CESM2-SF":
    
        combined_cases = []

        # Check for each of AAER, BMB, EE, GHG. Don't need to do xAER

        for forcing in ["AAER","BMB","EE","GHG"]:

            cases_tmp = [case for case in cases if forcing in case]

            case_numbers = [case[-3:] for case in cases_tmp]

            for case_number in case_numbers:

                # Select the cases with the same case id number

                combined_cases_tmp = [case for case in cases_tmp if case_number in case]

                combined_cases.append(delimiter.join(combined_cases_tmp))

        # Lastly, add in the xAER cases
        forcing = "xAER"
        cases_tmp = [case for case in cases if forcing in case]

        for case in cases_tmp:

            combined_cases.append(case)
            
    else:
        return np.unique(cases)
            
    return np.unique(combined_cases)


# ==============================================================================
# Main Function Call
# ==============================================================================
def main():
    
    # --------------------------------------------------------------------------
    # Initialize Logging
    # --------------------------------------------------------------------------    
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        encoding='utf-8', 
        level=20,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info("Generating List of Case Names")

    # --------------------------------------------------------------------------
    # Parse command line argument
    # -------------------------------------------------------------------------- 
    parser = argparse.ArgumentParser()

    parser.add_argument('--data_freq',type=str)
    parser.add_argument('--casenames_file',type=str)
    parser.add_argument('--ensemble_name',type=str)
 
    args = parser.parse_args()

    SAVE_FILE = args.casenames_file
    DATA_FREQ = args.data_freq    
    ENSEMBLE_NAME = args.ensemble_name.upper()

    
    # Check if specified ensemble is supported
    
    supported_ensembles = get_supported_ensembles()
    
    if ENSEMBLE_NAME in supported_ensembles:
        
        logging.info(f"Ensemble: {ENSEMBLE_NAME} is supported  by this application.")
    
    else:
        
        ensemble_logging_message = F'''
======================================================================================================================
!!!SPECIFIED ENSEMBLE NOT SUPPORTED!!!
======================================================================================================================
THE ENSEMBLE SPEICIFIED BY THE USER:
        ENSEMBLE_NAME={ENSEMBLE_NAME}
        
IS NOT CURRENTLY SUPPORTED BY THIS APPLICATION. CURRENTLY SUPPORTED ENSEMBLES:
{supported_ensembles}

IF YOU BELIEVE THIS IS IN ERROR, CONFIRM THE ENSEMBLE HAS BEEN ADDED TO BOTH:
    * get_supported_ensembles()
    * get_ensemble_data_path()

EXITING.
        '''
        
        logging.error(ensemble_logging_message)

        return    
    
    # Use OLR as an exmaple variable to generate casenames
    DATA_PATH = get_ensemble_data_path(ENSEMBLE_NAME) + DATA_FREQ + "/FLNT"
    
    # Generate case names
    cases = generate_case_names(DATA_PATH,)
    
    # Function which is necessary for certain ensembles but not for others
    cases = combine_split_cases(cases,ENSEMBLE_NAME)

    # --------------------------------------------------------------------------
    # Save case names to a text files
    # --------------------------------------------------------------------------

    with open(SAVE_FILE,mode='w') as write_file:
        for case in cases:
            write_file.writelines(case + "\n")

    logging.info(f"Case names saved to {SAVE_FILE}")

    # Return the file name to use in later part of the script
    return

if __name__ == "__main__":
    main()
    
