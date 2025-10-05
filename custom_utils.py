from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

class transformationss:
    
    def dedup(self,df:DataFrame,dedup_cols:List,cdc:str):

        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", row_number()\
                           .over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col('dedupCounts')==1)
        df = df.drop("dedupKey","dedupCounts")
        return df
    

    def process_timestamp(self,df):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df 
    
    def upsert(self,df,key_cols,table,cdc):
        merge_condistion = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condistion)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
                        .whenNotMatchedInsertAll()\
                        .execute()
        return 1
    