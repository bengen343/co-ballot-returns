import config *


#Calculate PVG scores for all voters in a dataframe
def calc_pvg(_df=voter_file_df):
    print("Calculating PVG & PVP values")

    # Add PVG Column
    _df['REGISTRATION_DATE'] = pd.to_datetime(voter_file_df['REGISTRATION_DATE'], exact=False, errors='coerce')
voter_file_df['PVG'] = 0

last_date = pd.to_datetime('12/01/2018')

voter_file_df['PVG'] = np.where(voter_file_df['REGISTRATION_DATE'] > last_date, 5,
                                voter_file_df[generals_list[0]] + voter_file_df[generals_list[1]] +
                                voter_file_df[generals_list[2]] + voter_file_df[generals_list[3]])

# Add PVP Column
voter_file_df['PVP'] = np.where(voter_file_df['REGISTRATION_DATE'] > last_date, 5,
                                voter_file_df[primaries_list[0]] + voter_file_df[primaries_list[1]] +
                                voter_file_df[primaries_list[2]] + voter_file_df[primaries_list[3]])

voter_file_df['PVG'].fillna(0, inplace=True)
voter_file_df['PVP'].fillna(0, inplace=True)

voter_file_df['PVG'] = 'PVG' + voter_file_df['PVG'].astype('str')
voter_file_df['PVP'] = 'PVP' + voter_file_df['PVP'].astype('str')

print("Finished adding PVG & PVP values at %s " % (datetime.strftime(datetime.now(), '%H:%M:%S')))