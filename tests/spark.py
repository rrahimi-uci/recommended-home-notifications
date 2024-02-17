import os
import sys
sys.path.append('.')

from pyspark.sql import SparkSession, SQLContext
from functools import lru_cache


@lru_cache(maxsize=None)
def get_spark():
    return (SparkSession.builder
                .master("local")
                .appName("recommended-notifications-local")
                .getOrCreate())
