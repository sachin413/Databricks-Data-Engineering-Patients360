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

allergy_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.allergy_raw")
diagnosis_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.diagnosis_raw")
medication_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.medication_raw")
transcript_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.transcript_raw")
transcriptallergy_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.transcriptallergy_raw")
transcriptdiagnosis_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.transcriptdiagnosis_raw")
transcriptmedication_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.transcriptmedication_raw")

prescription_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.prescription_raw")
immunization_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.immunization_raw")
labobservation_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.labobservation_raw")
labpanel_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.labpanel_raw")
labresult_raw_df=spark.sql("SELECT * FROM ey_team_40_bronze.labresult_raw")



patients_raw_df
patientcondition_raw_df
patientsmokingstatus_raw_df
condition_raw_df
smokingstatus_raw_df

#######
allergy_raw_df
diagnosis_raw_df
medication_raw_df
transcript_raw_df
transcriptallergy_raw_df
transcriptdiagnosis_raw_df
transcriptmedication_raw_df

########
prescription_raw_df
immunization_raw_df
labobservation_raw_df
labpanel_raw_df
labresult_raw_df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Call the data_cleaning_checks function

# COMMAND ----------

# DBTITLE 1,patients_cleaned_df
patients_cleaned_df = data_cleaning_checks(patients_raw_df, null_check_columns=["PatientGuid", "DMIndicator","Gender","YearOfBirth","State", "PracticeGuid"], unique_check_columns=["PatientGuid"])

# COMMAND ----------

# DBTITLE 1,patientcondition_cleaned_df
patientcondition_cleaned_df = data_cleaning_checks(patientcondition_raw_df, null_check_columns=["PatientConditionGuid", "PatientGuid","ConditionGuid","CreatedYear"], unique_check_columns=["PatientConditionGuid"])


# COMMAND ----------

# DBTITLE 1,patientsmokingstatus_cleaned_df
patientsmokingstatus_cleaned_df = data_cleaning_checks(patientsmokingstatus_raw_df, null_check_columns=["PatientSmokingStatusGuid", "PatientGuid","SmokingStatusGuid","EffectiveYear"], unique_check_columns=["PatientSmokingStatusGuid"])

# COMMAND ----------

# DBTITLE 1,condition_cleaned_df
condition_cleaned_df = data_cleaning_checks(condition_raw_df, null_check_columns=["ConditionGuid", "Code","Name"], unique_check_columns=["ConditionGuid"])

# COMMAND ----------

# DBTITLE 1,smokingstatus_cleaned_df
smokingstatus_cleaned_df = data_cleaning_checks(smokingstatus_raw_df, null_check_columns=["SmokingStatusGuid", "Description","NISTcode"], unique_check_columns=["SmokingStatusGuid"])

# COMMAND ----------

# DBTITLE 1,allergy_cleaned_df
allergy_cleaned_df = data_cleaning_checks(allergy_raw_df, null_check_columns=["AllergyGuid","PatientGuid", "AllergyType","MedicationNDCCode"], unique_check_columns=["AllergyGuid"])

# COMMAND ----------

diagnosis_cleaned_df =  data_cleaning_checks(diagnosis_raw_df, null_check_columns=["DiagnosisGuid","PatientGuid", "ICD9Code"], unique_check_columns=["DiagnosisGuid"])

# COMMAND ----------

medication_cleaned_df =  data_cleaning_checks(medication_raw_df, null_check_columns=["MedicationGuid","PatientGuid", "NdcCode","DiagnosisGuid"], unique_check_columns=["MedicationGuid"])

# COMMAND ----------

transcript_cleaned_df = data_cleaning_checks(transcript_raw_df, null_check_columns=["TranscriptGuid","PatientGuid", "VisitYear","Height","Weight","BMI","SystolicBP","DiastolicBP","HeartRate","RespiratoryRate","Temperature"], unique_check_columns=["TranscriptGuid"])


# COMMAND ----------

transcriptallergy_cleaned_df = data_cleaning_checks(transcriptallergy_raw_df, null_check_columns=["TranscriptGuid","TranscriptAllergyGuid","AllergyGuid" ], unique_check_columns=["TranscriptAllergyGuid"])

# COMMAND ----------

transcriptdiagnosis_cleaned_df = data_cleaning_checks(transcriptdiagnosis_raw_df, null_check_columns=["TranscriptGuid","TranscriptDiagnosisGuid","DiagnosisGuid" ], unique_check_columns=["TranscriptDiagnosisGuid"])

# COMMAND ----------

transcriptmedication_cleaned_df = data_cleaning_checks(transcriptmedication_raw_df, null_check_columns=["TranscriptGuid","TranscriptMedicationGuid","MedicationGuid" ], unique_check_columns=["TranscriptMedicationGuid"])

# COMMAND ----------

prescription_cleaned_df = data_cleaning_checks(prescription_raw_df, null_check_columns=["PrescriptionGuid","PatientGuid","MedicationGuid","PrescriptionYear" ], unique_check_columns=["PrescriptionGuid"])


# COMMAND ----------

immunization_cleaned_df = data_cleaning_checks(immunization_raw_df, null_check_columns=["ImmunizationGuid","PatientGuid","AdministeredYear" ], unique_check_columns=["ImmunizationGuid"])


# COMMAND ----------

labobservation_cleaned_df = data_cleaning_checks(labobservation_raw_df, null_check_columns=["LabObservationGuid","LabPanelGuid","HL7Identifier","HL7CodingSystem","ObservationYear" ], unique_check_columns=["LabObservationGuid"])


# COMMAND ----------

labpanel_cleaned_df = data_cleaning_checks(labpanel_raw_df, null_check_columns=["LabPanelGuid","LabResultGuid","PanelName"], unique_check_columns=["LabPanelGuid"])

# COMMAND ----------

labresult_cleaned_df = data_cleaning_checks(labresult_raw_df, null_check_columns=["LabResultGuid","PatientGuid","TranscriptGuid","PracticeGuid","ReportYear" ], unique_check_columns=["LabResultGuid"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table from cleaned dataframes

# COMMAND ----------

# DBTITLE 1,Create table from cleaned dataframes
patients_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patients")

patientcondition_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patient_condition")

patientsmokingstatus_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.patient_smokingstatus")

condition_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.medical_condition")

smokingstatus_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.smoking_status")

allergy_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.allergy")

diagnosis_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.diagnosis")

medication_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.medication")

transcript_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.transcript")

transcriptallergy_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.transcript_allergy")

transcriptdiagnosis_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.transcript_diagnos")

transcriptmedication_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.transcript_medication")

prescription_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.prescription")

immunization_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.immunization")

labobservation_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.lab_observation")

labpanel_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.lab_panel")

labresult_cleaned_df.write.mode("overwrite").saveAsTable("ey_team_40_silver.lab_result")


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ey_team_40_silver
