import math
import re

import numpy as np
import pandas as pd

def round_price(price:int, step:int, to_lower:bool=True):
    if pd.isna(price):  # skip NaN
        return np.nan
    q = price / step
    return (math.floor(q) if to_lower else math.ceil(q)) * step

def format_part_number(pn: str) -> str:
    pn = str(pn).strip()

    if pn == "" or pn.lower() == "nan":
        return pn

    if pn[0].isdigit():
        # starts with a digit → xx.xxxx.xx
        first = pn[:2].upper()
        second = pn[2:6].replace(" ", "")
        third = pn[6:].replace(" ", "")
        # pad if too short
        return f"{first}.{second}.{third}"
    else:
        # starts with a letter → x xx xxx
        first = pn[0].upper()
        rest = pn[1:].replace(" ", "")
        # pad if too short
        rest = rest.ljust(5, "0")
        return f"{first} {rest[:2]} {rest[2:5]}"

def main():
    df = pd.read_csv('Data/Prices.csv')
    df.rename(columns={
        " MPC ": "mpc",
        " SO NASA MARZA ": "with margin",
        "ZALIHA": "quantity",
        "PROGRAMA ": "programa",
        "CATALOGUE PART NUMBER": "catalogue_part_number"
    }, inplace=True)

    # Fallback for missing PART NUMBER
    # df["raw_part_number"] = df["PART NUMBER"].fillna(df["catalogue_part_number"])

    # Clean part numbers
    df["part_number"] = df["PART NUMBER"].apply(format_part_number)

    # Clean MPC
    df["mpc"] = pd.to_numeric(
        df["mpc"].astype(str).str.replace(",", "", regex=False),
        errors="coerce"
    )

    # Rounding
    df["final_price_lower"] = df["mpc"].apply(lambda x: round_price(x, step=50, to_lower=True))

    # Final price: programa if available, else rounded lower
    df["programa"] = pd.to_numeric(df["programa"], errors="coerce")
    df["final_price"] = df["programa"].combine_first(df["final_price_lower"])

    # Clean quantity
    df["quantity"] = df["quantity"].fillna(0)

    # Select only required columns
    df_cleaned = df[["part_number", "quantity", "mpc", "final_price"]]

    # Save
    df_cleaned.to_csv('Data/Prices_cleaned.csv', index=False)




if __name__ == "__main__":
    main()