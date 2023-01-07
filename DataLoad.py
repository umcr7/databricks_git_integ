# Databricks notebook source
!pwd

# COMMAND ----------

data = spark.read.format("csv")\
            .load("file:/Workspace/Repos/muhammad.umar@wolfofdata.com/databricks_git_integ/fifa_dataset/team_data.csv",
                 header=True)

# COMMAND ----------

data.display()

# COMMAND ----------

data.write.saveAsTable("team_data")

# COMMAND ----------

spark.table("team_data")

# COMMAND ----------

spark.table("team_data").display()


