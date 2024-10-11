# Databricks notebook source
# MAGIC %fs
# MAGIC ls /trainingSet/

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion

# COMMAND ----------

# DBTITLE 1,Create DF - training_SyncPatient
patients_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncPatient.csv')
patients_df

# COMMAND ----------

# DBTITLE 1,Create table -  Patient_Raw
patients_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Patient_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - training_SyncPatientCondition
patients_condition_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncPatientCondition.csv')
patients_condition_df

# COMMAND ----------

# DBTITLE 1,Create table - PatientCondition_Raw
patients_condition_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.PatientCondition_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - training_SyncPatientSmokingStatus
patients_smoking_status_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncPatientSmokingStatus.csv')
patients_smoking_status_df

# COMMAND ----------

# DBTITLE 1,Create table - PatientSmokingStatus_Raw
patients_smoking_status_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.PatientSmokingStatus_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncSmokingStatus
smoking_status_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/SyncSmokingStatus.csv')
smoking_status_df

# COMMAND ----------

# DBTITLE 1,Create table - SmokingStatus_Raw
smoking_status_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.SmokingStatus_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncCondition
condition_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/SyncCondition.csv')
condition_df

# COMMAND ----------

# DBTITLE 1,Create table - Condition_Raw
condition_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Condition_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncAllergy
allergy_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncAllergy.csv')
allergy_df

# COMMAND ----------

# DBTITLE 1,Create table - Allergy_Raw
allergy_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Allergy_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncDiagnosis
diagnosis_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncDiagnosis.csv')
diagnosis_df

# COMMAND ----------

# DBTITLE 1,Create table - Diagnosis_Raw
diagnosis_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Diagnosis_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncImmunization
immunization_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncImmunization.csv')
immunization_df

# COMMAND ----------

# DBTITLE 1,Create table - Immunization_Raw
immunization_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Immunization_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncLabObservation
labobservation_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncLabObservation.csv')
labobservation_df

# COMMAND ----------

# DBTITLE 1,Create table - LabObservation_Raw
labobservation_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.LabObservation_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncLabPanel
labpanel_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncLabPanel.csv')
labpanel_df


# COMMAND ----------

# DBTITLE 1,Create table - LabPanel_Raw
labpanel_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.LabPanel_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncLabResult
labresult_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncLabResult.csv')
labresult_df

# COMMAND ----------

# DBTITLE 1,Create table - LabResult_Raw
labresult_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.LabResult_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF - SyncMedication
medication_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncMedication.csv')
medication_df


# COMMAND ----------

# DBTITLE 1,Create table - Medication_Raw
medication_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Medication_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF -SyncPrescription
prescription_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncPrescription.csv')
prescription_df


# COMMAND ----------

# DBTITLE 1,Create table - Prescription_Raw
prescription_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Prescription_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF -SyncTranscript
transcript_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncTranscript.csv')
transcript_df

# COMMAND ----------

# DBTITLE 1,Create table - Transcript_Raw
transcript_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.Transcript_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF -SyncTranscriptAllergy
transcriptallergy_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncTranscriptAllergy.csv')
transcriptallergy_df


# COMMAND ----------

# DBTITLE 1,Create table - TranscriptAllergy_Raw
transcriptallergy_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.TranscriptAllergy_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF -SyncTranscriptDiagnosis
transcriptdiagnosis_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncTranscriptDiagnosis.csv')
transcriptdiagnosis_df

# COMMAND ----------

# DBTITLE 1,Create table - TranscriptDiagnosis_Raw
transcriptdiagnosis_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.TranscriptDiagnosis_Raw')

# COMMAND ----------

# DBTITLE 1,Create DF -SyncTranscriptMedication
transcriptmedication_df= spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/trainingSet/training_SyncTranscriptMedication.csv')
transcriptmedication_df


# COMMAND ----------

# DBTITLE 1,Create table - TranscriptMedication_Raw
transcriptmedication_df.write.mode('overwrite').saveAsTable('ey_team_40_bronze.TranscriptMedication_Raw')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ey_team_40_bronze
