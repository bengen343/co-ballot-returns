import multiprocessing

import pandas as pd

from config import *


def append_result(result_df: pd.DataFrame):
    global crosstabs_df
    crosstabs_df = pd.concat([crosstabs_df, result_df], axis=0, sort=False)


def vertical_crosstab(vertical: str, crosstab_criteria_lst: list, df: pd.DataFrame) -> pd.DataFrame:
    horizontal_df = pd.DataFrame()
    print(f"Running cross tabs on vertical criteria: {vertical}")
    
    for horizontal in crosstab_criteria_lst:
        horizontal_df = pd.concat([horizontal_df, pd.crosstab(df[vertical], df[horizontal], margins=True)], axis=1, sort=True)
        del horizontal_df['All']
        
    return horizontal_df


def async_crosstabs(crosstab_criteria_lst: list, df: pd.DataFrame):
    print(f"Starting crosstab calculation.")
    pool = multiprocessing.Pool()
    vertical_crosstab_lst = crosstab_criteria_lst + ['PRECINCT']
    
    for vertical in vertical_crosstab_lst:
        pool.apply_async(vertical_crosstab, args=(vertical, crosstab_criteria_lst, df), callback=append_result)
    pool.close()
    pool.join()
    
    return crosstabs_df
