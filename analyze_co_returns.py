from config import *
import pandas as pd

# Compose crosstags on given voter file data frame
def calc_crosstabs(_df, crosstab_criteria_lst):

    _crosstabs_df = pd.DataFrame()
    
    for vertical in crosstab_criteria_lst:
        _horizontal_df = pd.DataFrame()
        
        for horizontal in crosstab_criteria_lst:
            # print(f"Vertical: {vertical} | Horizontal: {horizontal}")
            try:
                _horizontal_df = pd.concat([_horizontal_df, pd.crosstab(_df[vertical], _df[horizontal], margins=True)], axis=1, sort=True)
                del _horizontal_df['All']
            except Exception as e:
                print(e)

        _horizontal_df = _horizontal_df.loc[:,~_horizontal_df.columns.duplicated(keep=False)].copy()
        _crosstabs_df = pd.concat([_crosstabs_df, _horizontal_df], axis=0, sort=False)

    # Add the precinct crosstabs only to the vertical crosstab axis
    _horizontal_df = pd.DataFrame()
    
    for horizontal in crosstab_criteria_lst:
        try:
            _horizontal_df = pd.concat([_horizontal_df, pd.crosstab(_df['PRECINCT'], _df[horizontal], margins=True)], axis=1, sort=True)
            del _horizontal_df['All']
        except Exception as e:
            print(e)

    _horizontal_df = _horizontal_df.loc[:,~_horizontal_df.columns.duplicated(keep=False)].copy()
    _crosstabs_df = pd.concat([_crosstabs_df, _horizontal_df], axis=0, sort=False)

    _crosstabs_df = _crosstabs_df[_crosstabs_df.index != 'All']

    return _crosstabs_df