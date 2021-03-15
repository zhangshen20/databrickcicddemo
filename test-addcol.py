# Databricks notebook source
# test-addcol.py
import pytest
import pandas as pd
from addcol import with_status


# from dbxdemo.spark import get_spark
# from dbxdemo.appendcol import with_status

# def with_status(df):
# 	# return df.withColumn("status", F.lit("checked"))
#     return df.copy()

class TestAppendCol(object):

  def test_with_status(self):
    source_data = [
        ["paula", "white", "paula.white@example.com"],
        ["john",  "baer",  "john.baer@example.com"]
    ]
    # source_df = get_spark().createDataFrame(
    #     source_data,
    #     ["first_name", "last_name", "email"]
    # )

    source_df = pd.DataFrame(source_data, columns = ['firstname','lastname', 'email'])

    actual_df = with_status(source_df)

    print(actual_df)

    expected_data = [
        ["paula", "white", "paula.white@example.com"],
        ["john",  "baer",  "john.baer@example.com"]
    ]

    expected_df = pd.DataFrame(expected_data, columns = ['firstname','lastname', 'email'])

    assert(expected_df['firstname'][0] == source_df['firstname'][0])
    # assert(1 == 1)
