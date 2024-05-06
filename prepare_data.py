import pandas as pd

# Load your data
inverter_data = pd.read_csv('data/original/inverter.csv', sep=',', decimal='.', quotechar='"')
boiler_data = pd.read_csv('data/original/boiler.csv', sep=',', decimal='.', quotechar='"')
electricity_price_data = pd.read_csv('data/original/electricity-price.csv', sep=';', decimal=',', quotechar='"', usecols=['NPS Eesti'], encoding='iso-8859-1')


selected_columns = inverter_data[['ts', 'inv_pv_power2', 'inv_bat_power1', 'inv_bat_soc1']]
grid_power_total = inverter_data[['power1', 'power2', 'power3']]


# Rename the columns for the output
selected_columns.columns = ['date', 'pv_power', 'bat_power', 'bat_soc']

# Sum the selected columns into a new column
selected_columns['grid_power'] = grid_power_total.sum(axis=1)
selected_columns['ewh_power'] = boiler_data['power1']

electricity_price_est = electricity_price_data[['NPS Eesti']]

# Repeat each row value 60 times = 60 minutes per hour
# Explode the list created by repeating the values to expand them into a DataFrame
# 1000/60000 transforms price value from MWh to W/s
repeated_df = electricity_price_data['NPS Eesti'].apply(lambda x: [x/1000/60000] * 60).explode().reset_index(drop=True)
selected_columns['price_w_min'] = repeated_df


def calc_power_input(row):
    return (abs(row['bat_power']) if row['bat_power'] < 0 else 0) + row['grid_power']+row['pv_power']

def calc_power_load(row):
    return (abs(row['bat_power']) if row['bat_power'] > 0 else 0) + row['ewh_power']

def calc_not_tracked_load(row):
    return row['power_input'] - row['power_load']

def calc_total_load(row):
    return row['not_tracked_load'] + row['power_load']

def calc_grid_cost(row):
    return row['grid_power'] * row['price_w_min']

def calc_pv_cost_save(row):
    return row['pv_power'] * (0 if row['price_w_min'] < 0 else row['price_w_min'])

def calc_bat_discharge_win(row):
     return (abs(row['bat_power']) if row['bat_power'] < 0 else 0) * (0 if row['price_w_min'] < 0 else row['price_w_min'])

def calc_bat_charge_cost(row):
     return (row['bat_power']-row['pv_power'] if row['bat_power'] > 0 else 0) * (0 if row['price_w_min'] < 0 else row['price_w_min'])

def calc_cost_savings(row):
     return row['bat_discharge_win'] + row['pv_cost_save']



# Apply the function across the DataFrame rows
selected_columns['power_input'] = selected_columns.apply(calc_power_input, axis=1)
selected_columns['power_load'] = selected_columns.apply(calc_power_load, axis=1)
selected_columns['not_tracked_load'] = selected_columns.apply(calc_not_tracked_load, axis=1)
selected_columns['total_load'] = selected_columns.apply(calc_total_load, axis=1)
selected_columns['grid_cost'] = selected_columns.apply(calc_grid_cost, axis=1)
selected_columns['pv_cost_save'] = selected_columns.apply(calc_pv_cost_save, axis=1)
selected_columns['bat_discharge_win'] = selected_columns.apply(calc_bat_discharge_win, axis=1)
selected_columns['bat_charge_cost'] = selected_columns.apply(calc_bat_charge_cost, axis=1)
selected_columns['cost_savings'] = selected_columns.apply(calc_cost_savings, axis=1)



# this will fill missing values with with 0 - needs better approach because it causes flaws in final output
selected_columns = selected_columns.fillna(0)


selected_columns.to_csv('data/prepared/output.csv', index=False)

