from pyspark.sql.functions import *

class SilverTransformation:

    def __init__(self, df=None):
        self.df = df

    def add_cdc_column(self, cdc_column):
        self.df = self.df.withColumn(cdc_column, current_timestamp())
        return self.df

    def drop_duplicates(self, specific_column):
        self.df = self.df.dropDuplicates(specific_column)
        return self.df

    def fill_all_nullsStr(self, col_names):
        for c in col_names:
            self.df = self.df.fillna({c: "N/A"})
        return self.df

    def fill_all_nullsInt(self, col_names):
        for c in col_names:
            self.df = self.df.fillna({c: 0})
        return self.df

    def drop_data(self, cols):
        self.df = self.df.drop(*cols)
        return self.df
