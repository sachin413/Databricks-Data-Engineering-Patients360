# Databricks notebook source
# DBTITLE 1,Check null values and validate unique values
from pyspark.sql.functions import col, isnan, when, count

def data_cleaning_checks(source_df, null_check_columns=[], unique_check_columns=[]):
    # Check for null values
    null_counts = source_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in null_check_columns]).collect()[0]
    null_columns = [column for column, count in null_counts.asDict().items() if count > 0]
    
    # Check for unique values
    unique_columns = [column for column in unique_check_columns if source_df.select(column).distinct().count() != source_df.count()]
    
    if null_columns or unique_columns:
        print("Data cleaning checks failed:")
        if null_columns:
            print("Null values found in columns:", null_columns)
        if unique_columns:
            print("Columns not having all unique values:", unique_columns)
    else:
        print("Data cleaning checks passed.")
        target_df = source_df  # Create target dataframe if all checks pass
        return target_df

# COMMAND ----------

patients_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.patient_Raw")
patientcondition_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.patientcondition_raw")
patientsmokingstatus_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.patientsmokingstatus_raw")
condition_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.condition_raw")
smokingstatus_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.smokingstatus_raw")

patients_raw_df
patientcondition_raw_df
patientsmokingstatus_raw_df
condition_raw_df
smokingstatus_raw_df

# COMMAND ----------

# DBTITLE 1,call data cleaning check function and create cleaned dataframes

# Call the data_cleaning_checks function
patients_cleaned_df = data_cleaning_checks(patients_raw_df, null_check_columns=["PatientGuid", "DMIndicator","Gender","YearOfBirth","State", "PracticeGuid"], unique_check_columns=["PatientGuid"])

patientcondition_cleaned_df = data_cleaning_checks(patientcondition_raw_df, null_check_columns=["PatientConditionGuid", "PatientGuid","ConditionGuid","CreatedYear"], unique_check_columns=["PatientConditionGuid"])

patientsmokingstatus_cleaned_df = data_cleaning_checks(patientsmokingstatus_raw_df, null_check_columns=["PatientSmokingStatusGuid", "PatientGuid","SmokingStatusGuid","EffectiveYear"], unique_check_columns=["PatientSmokingStatusGuid"])

condition_cleaned_df = data_cleaning_checks(condition_raw_df, null_check_columns=["ConditionGuid", "Code","Name"], unique_check_columns=["ConditionGuid"])

smokingstatus_cleaned_df = data_cleaning_checks(smokingstatus_raw_df, null_check_columns=["SmokingStatusGuid", "Description","NISTcode"], unique_check_columns=["SmokingStatusGuid"])



# COMMAND ----------

# DBTITLE 1,Create table from cleaned dataframes
patients_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patients")

patientcondition_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patient_condition")

patientsmokingstatus_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patient_smokingstatus")

condition_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.medical_condition")

smokingstatus_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.smoking_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ey_team_40_silver
