import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class PopulationProcessor:
    def __init__(self, files_and_states, output_path="output.csv"):
        self.files_and_states = files_and_states  # List of tuples: (filepath, sheet_name, state_name)
        self.output_path = output_path
        self.full_data = pd.DataFrame()

    def load_and_transform(self, file_path, sheet_name, state_name):
        try:
            logging.info(f"Processing file for {state_name}: {os.path.basename(file_path)}")
            xls = pd.ExcelFile(file_path)
            df = xls.parse(sheet_name, skiprows=3)

            # Drop state total row
            df = df.iloc[1:].copy()

            # Rename and reshape
            df.columns = ['County', 'Base_Pop', '2020', '2021', '2022', '2023', '2024']
            df = df.drop(columns='Base_Pop')
            df['County'] = df['County'].str.replace(r'^\.', '', regex=True)
            df['County'] = df['County'].str.replace(f', {state_name}', '', regex=False)

            df_long = df.melt(id_vars=['County'], var_name='Year', value_name='Population')
            df_long['Year'] = df_long['Year'].astype(int)
            df_long['Population'] = df_long['Population'].fillna(0).astype(int)
            df_long['State'] = state_name
            return df_long

        except Exception as e:
            logging.error(f"Failed to process {state_name}: {e}")
            return pd.DataFrame()

    def regress_backcast(self, df, start_year=2005, end_year=2019):
        projections = []
        logging.info("Performing regression projections...")

        for county in df['County'].unique():
            sub_df = df[df['County'] == county]
            X = sub_df['Year'].values.reshape(-1, 1)
            y = sub_df['Population'].values
            model = LinearRegression()
            model.fit(X, y)

            for year in range(start_year, end_year + 1):
                projected = int(model.predict(np.array([[year]]))[0])
                projections.append({
                    'State': sub_df['State'].iloc[0],
                    'County': county,
                    'Year': year,
                    'Population': projected
                })

        proj_df = pd.DataFrame(projections)
        return proj_df

    def run(self):
        all_states_df = []

        for file_path, sheet_name, state_name in self.files_and_states:
            cleaned = self.load_and_transform(file_path, sheet_name, state_name)
            if not cleaned.empty:
                projected = self.regress_backcast(cleaned)
                merged = pd.concat([projected, cleaned], ignore_index=True)
                all_states_df.append(merged)

        if all_states_df:
            self.full_data = pd.concat(all_states_df, ignore_index=True)
            self.full_data.sort_values(by=['State', 'County', 'Year'], inplace=True)
            self.full_data.to_csv(self.output_path, index=False)
            logging.info(f"Output written to {self.output_path}")
        else:
            logging.warning("No data processed.")


# === USAGE ===
if __name__ == "__main__":
    files = [
        ("co-est2024-pop-37.xlsx", "CO-EST2024-POP-37", "North Carolina"),
        ("co-est2024-pop-45.xlsx", "CO-EST2024-POP-45", "South Carolina"),
        ("co-est2024-pop-47.xlsx", "CO-EST2024-POP-47", "Tennessee"),
        ("co-est2024-pop-51.xlsx", "CO-EST2024-POP-51", "Virginia")
    ]

    processor = PopulationProcessor(files, output_path="CHANGEME.csv")
    processor.run()
