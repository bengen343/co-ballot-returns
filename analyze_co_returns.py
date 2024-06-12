import multiprocessing

import pandas as pd

from config import *


def vertical_crosstab(vertical: str, crosstab_criteria_lst: list, df: pd.DataFrame) -> pd.DataFrame:
    horizontal_df = pd.DataFrame()
    print(f"Running cross tabs on vertical criteria: {vertical}")
    
    for horizontal in crosstab_criteria_lst:
        try:
            horizontal_df = pd.concat([horizontal_df, pd.crosstab(df[vertical], df[horizontal], margins=True)], axis=1, sort=True)
            del horizontal_df['All']
        except Exception as e:
            if 'concatenate' in str(e):
                print(e)
            else:
                raise
        
    return horizontal_df


def print_error(e):
    print(e)


def async_crosstabs(crosstab_criteria_lst: list, df: pd.DataFrame):
    processes_int = 4
    crosstabs_df = pd.DataFrame()
    vertical_crosstab_lst = crosstab_criteria_lst + ['PRECINCT']
    print(f"Starting crosstab calculation with {processes_int} workers.")
    
    pool = multiprocessing.Pool(processes=processes_int)
    
    results = [pool.apply_async(vertical_crosstab, args=(x, crosstab_criteria_lst, df), error_callback=print_error) for x in vertical_crosstab_lst]
    pool.close()
    pool.join()
    crosstabs_df = pd.concat([p.get() for p in results], axis=0, sort=False)
    
    return crosstabs_df
