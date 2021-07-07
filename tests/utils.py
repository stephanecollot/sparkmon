"""Test utils."""
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get Spark Session."""
    spark = SparkSession.builder.getOrCreate()
    return spark


def get_random_df(size: int = 100) -> pd.DataFrame:
    """Create random dataframe."""
    df = pd.DataFrame(np.random.randint(0, size, size=(size, 4)), columns=list("ABCD"))
    return df
