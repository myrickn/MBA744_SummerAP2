import pandas as pd

# Load each CSV file
income_df = pd.read_csv('income.csv')
cpi_df = pd.read_csv('cpi.csv')
population_df = pd.read_csv('Population.csv')
unemployment_df = pd.read_csv('unemployment.csv')

# Merge all dataframes with left join
merged_df = income_df.merge(cpi_df, on='Year', how='left')
merged_df = merged_df.merge(population_df, on='Year', how='left')
merged_df = merged_df.merge(unemployment_df, on='Year', how='left')

# sort the merged dataset by Year
merged_df.sort_values(by='Year', inplace=True)

# Save the result to a new CSV file
merged_df.to_csv('merged_dataset.csv', index=False)

print("Merged dataset saved as merged_dataset.csv")
