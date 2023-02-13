#!/usr/bin/env python3
"""
Can run pydoctest on this module with the following command:
python -m doctest -v spend_analysis.py
"""

import argparse
import doctest  # noqa: F401  # pylint: disable=W0611
import re
import sys
from datetime import datetime
from io import StringIO  # noqa: F401 # pylint: disable=W0611

import pandas as pd


def lookup(description, lookups):
    """
    Parameters :
        description (str): The description of the transaction we wish to find the category for.
        lookups (pandas.dataframe): A two column table with regular expressions and categories.

    Returns:
         category (str): The category description belongs to.

    >>> csv_string = '''
    ... regex,category
    ... ALDI,food
    ... AMAZING THREADS,merchandise
    ... '''
    >>> csv_string_io = StringIO(csv_string)
    >>> lookups_df = pd.read_csv(csv_string_io)
    >>> lookup("ALDI 72083 CRYSTAL MN", lookups_df)
    'food'

    >>> lookup("THE AMAZING THREADS INC", lookups_df)
    'merchandise'
    """
    for row in lookups.itertuples(index=False):
        if re.search(row.regex, description, re.IGNORECASE):
            return row.category
    return "unknown"


def normalize_citi(dataframe):
    """
    Example citi csv that we read in.
    >>> csv_string = '''
    ... Status,Date,Description,Debit,Credit,Member Name
    ... Cleared,01/28/2023,BROOKLYN PARK PET HOSPITABROOKLYN PARKMN,14.52,,BOB JHONES
    ... Cleared,12/05/2022,THE HOME DEPOT #2804 BROOKLYN PARKMN,,-7.49,JILL JHONES
    ... '''
    >>> csv_string_io = StringIO(csv_string)


    This is what the passed in dataframe looks like
    >>> df = pd.read_csv(csv_string_io, header=None)
    >>> df
             0           1                                         2      3       4            5
    0   Status        Date                               Description  Debit  Credit  Member Name
    1  Cleared  01/28/2023  BROOKLYN PARK PET HOSPITABROOKLYN PARKMN  14.52     NaN   BOB JHONES
    2  Cleared  12/05/2022      THE HOME DEPOT #2804 BROOKLYN PARKMN    NaN   -7.49  JILL JHONES


    Now we transform this into our normalized form.
    >>> normalized_df = normalize_citi(df)
    >>> normalized_df
    0        Date  Amount                               Description
    1  01/28/2023   14.52  BROOKLYN PARK PET HOSPITABROOKLYN PARKMN
    2  12/05/2022   -7.49      THE HOME DEPOT #2804 BROOKLYN PARKMN

    """

    df = dataframe.copy()
    # Set the column names with the values that are in the first row.
    df.columns = df.iloc[0]
    # Remove the first row, as they contained the column names.
    df = df[1:]
    # Force the Debit and Credit columns to be numeric values. (they may have been read in as strings because of the first row)
    df["Debit"] = pd.to_numeric(df["Debit"])
    df["Credit"] = pd.to_numeric(df["Credit"])
    # The csv we read in has blanks, instead of zero values, some make all those zero instead of NaN.
    df.fillna(0, inplace=True)
    # We want to reduce the Credit and Debit columns into a single "Amount" column.
    df["Amount"] = df["Credit"] + df["Debit"]
    # Cast off the columns that have no value to us.
    df.drop(columns=["Status", "Member Name", "Debit", "Credit"], inplace=True)
    # Re-arrange the columns to this order.
    df = df[["Date", "Amount", "Description"]]
    return df


def normalize_discover(dataframe):
    """

        Example discover csv that we read in.
    >>> csv_string = '''
    ... Trans. Date,Post Date,Description,Amount,Category
    ... 01/10/2022,01/11/2022,"THE HOME DEPOT #0509 AUSTIN TX",27.03,"Home Improvement"
    ... 01/11/2022,01/11/2022,"NEW TECH TENNIS AUSTIN TX",35.32,"Merchandise"
    ... '''
    >>> csv_string_io = StringIO(csv_string)

        This is what the passed in dataframe looks like
    >>> df = pd.read_csv(csv_string_io, header=None)
    >>> df
                 0           1                               2       3                 4
    0  Trans. Date   Post Date                     Description  Amount          Category
    1   01/10/2022  01/11/2022  THE HOME DEPOT #0509 AUSTIN TX   27.03  Home Improvement
    2   01/11/2022  01/11/2022       NEW TECH TENNIS AUSTIN TX   35.32       Merchandise


    Now we transform this into our normalized form.
    >>> normalized_df = normalize_discover(df)
    >>> normalized_df
    0        Date                     Description  Amount
    1  01/10/2022  THE HOME DEPOT #0509 AUSTIN TX   27.03
    2  01/11/2022       NEW TECH TENNIS AUSTIN TX   35.32

    """

    df = dataframe.copy()
    df.columns = df.iloc[0]
    df = df[1:]
    df.drop(columns=["Post Date", "Category"], inplace=True)
    df.rename(columns={"Trans. Date": "Date"}, inplace=True)
    df = df[["Date", "Description", "Amount"]]
    df["Amount"] = pd.to_numeric(df["Amount"])
    return df


def normalize_wellsfargo(dataframe):
    """
        Example wells fargo csv that we read in
    >>> csv_string = '''
    ... "01/10/2023","-7.24","*","","Audible*8931J9V53 Amzn.com/billNJ"
    ... "01/04/2023","-206.36","*","","STATE FARM INSURANCE 800-956-6310 IL"
    ... '''
    >>> csv_string_io = StringIO(csv_string)
    >>> df = pd.read_csv(csv_string_io, header=None)
    >>> df
                0       1  2   3                                     4
    0  01/10/2023   -7.24  * NaN     Audible*8931J9V53 Amzn.com/billNJ
    1  01/04/2023 -206.36  * NaN  STATE FARM INSURANCE 800-956-6310 IL



    Now we normalize
    >>> normalized_df = normalize_wellsfargo(df)
    >>> normalized_df
             Date  Amount                           Description
    0  01/10/2023   -7.24     Audible*8931J9V53 Amzn.com/billNJ
    1  01/04/2023 -206.36  STATE FARM INSURANCE 800-956-6310 IL

    """
    df = dataframe.copy()
    df.drop(columns=[2, 3], inplace=True)
    df.columns = ["Date", "Amount", "Description"]
    return df


def main():
    """
    The main entry point when this module is run as a scrpt.
    """
    lookup_df = pd.read_csv("spend_analysis.csv")

    parser = argparse.ArgumentParser(
        description="Takes in transaction history from csv files and aggrigates them into one csv called out.csv"
    )
    parser.add_argument(
        "files", nargs="+", type=argparse.FileType("r"), default=sys.stdin
    )

    args = parser.parse_args()

    transaction_dataframes = []

    for file in args.files:
        # logic for figuring out what format the csv is in (citi, discover, wells fargo, etc)
        df = pd.read_csv(file, header=None)
        if "Status" == df[0][0]:
            df = normalize_citi(df)
            transaction_dataframes.append(df)
        elif "Trans. Date" == df[0][0]:
            df = normalize_discover(df)
            transaction_dataframes.append(df)
        else:
            try:
                datetime.strptime(df[0][0], "%m/%d/%Y").date()
                df = normalize_wellsfargo(df)
                transaction_dataframes.append(df)
            except ValueError:
                print("Error: Unknown format from", file.name)

    result = pd.concat(transaction_dataframes)
    result["Category"] = result["Description"].apply(lambda x: lookup(x, lookup_df))

    result.to_csv("out.csv", index=False)


if __name__ == "__main__":
    main()
