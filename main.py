from load_co_returns import unzip, returns_to_df, voters_to_df, vote_history_to_df
from config import *
import pandas as pd


unzip(_file=return_zip)
returns_df = returns_to_df(return_txt_file)

voters_df = voters_to_df(bq_query_str=bq_voter_str)

vote_history_df = vote_history_to_df(bq_query_str=bq_history_str)

voters_df = pd.merge(voters_df, returns_df, how='left', on='VOTER_ID')
voters_df = pd.merge(voters_df, vote_history_df, how='left', on='VOTER_ID')

print(len(voters_df))
