import sys
import pandas as pd

TESTING = True


def main():
    complaints_file = sys.argv[1]
    zip_file = sys.argv[2]
    output_file = sys.argv[3]

    # Read data and preprocess
    nrows = 1000 if TESTING else None
    rename_dict = {
        "Created Date": "date",
        "Complaint Type": "type",
        "Descriptor": "descriptor",
        "Incident Zip": "zip",
    }
    df = pd.read_csv(complaints_file, usecols=list(rename_dict.keys()),
                    nrows=nrows, parse_dates=["Created Date"])
    df = df.rename(columns=rename_dict)
    zips = pd.read_csv(zip_file)

    # Filter valid dates and then join
    df.query("date >= '12/01/2010' and date < '11/01/2020'", inplace=True)
    merged = pd.merge(df, zips, on="zip", how="inner").drop(columns=["zip"])
    merged.to_csv(output_file, index=False,
                  date_format="%m/%d/%Y %I:%M:%S %p", na_rep="null")


if __name__ == '__main__':
    main()
