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


def async_crosstabs(crosstab_criteria_lst: list, df: pd.DataFrame):
    print(f"Starting crosstab calculation.")
    crosstabs_df = pd.DataFrame()

    pool = multiprocessing.Pool()
    vertical_crosstab_lst = crosstab_criteria_lst + ['PRECINCT']
    
    for vertical in vertical_crosstab_lst:
        result = pool.apply_async(vertical_crosstab, args=(vertical, crosstab_criteria_lst, df))
        crosstabs_df = pd.concat([crosstabs_df, result.get()], axis=0, sort=False)
    pool.close()
    pool.join()
    
    return crosstabs_df
