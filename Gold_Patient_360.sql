-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## One time script for creating dimesions table in gold with current timestamp and CurrentFlag columns.

-- COMMAND ----------

/*CREATE TABLE ey_team_40_gold.d_patients AS 
(SELECT *
  ,CURRENT_TIMESTAMP() as LoadDate
  ,TRUE as CurrentFlag
  FROM ey_team_40_silver.patients)*/

-- COMMAND ----------

/*CREATE TABLE ey_team_40_gold.d_medical_condition AS 
(SELECT *
  ,CURRENT_TIMESTAMP() as LoadDate
  ,TRUE as CurrentFlag
  FROM ey_team_40_silver.medical_condition)*/

-- COMMAND ----------

/*CREATE TABLE ey_team_40_gold.d_smoking_status AS 
(SELECT *
  ,CURRENT_TIMESTAMP() as LoadDate
  ,TRUE as CurrentFlag
  FROM ey_team_40_silver.smoking_status)*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Snapshots of Dimension tables, Upsert operation to maintain historical records

-- COMMAND ----------

-- DBTITLE 1,d_patients
MERGE INTO ey_team_40_gold.d_patients tgt
USING ey_team_40_silver.patients src
ON tgt.PatientGuid = src.PatientGuid
WHEN MATCHED AND (tgt.DMIndicator <> src.DMIndicator OR
                  tgt.Gender <> src.Gender OR
                  tgt.YearOfBirth <> src.YearOfBirth OR
                  tgt.State <> src.State OR
                  tgt.PracticeGuid <> src.PracticeGuid)
THEN
  UPDATE SET tgt.CurrentFlag = FALSE -- Mark the existing record's CurrentFlag as False
WHEN NOT MATCHED THEN
  INSERT (PatientGuid, DMIndicator, Gender, YearOfBirth, State, PracticeGuid, LoadDate, CurrentFlag)
  VALUES (src.PatientGuid, src.DMIndicator, src.Gender, src.YearOfBirth, src.State, src.PracticeGuid, CURRENT_TIMESTAMP(), TRUE);

-- COMMAND ----------

-- DBTITLE 1,d_medical_condition
MERGE INTO ey_team_40_gold.d_medical_condition tgt
USING ey_team_40_silver.medical_condition src
ON tgt.ConditionGuid = src.ConditionGuid
WHEN MATCHED AND (tgt.Code <> src.Code OR
                  tgt.Name <> src.Name )
THEN
  UPDATE SET tgt.CurrentFlag = FALSE -- Mark the existing record's CurrentFlag as False
WHEN NOT MATCHED THEN
  INSERT (ConditionGuid,Code,Name,LoadDate,CurrentFlag)
  VALUES (src.ConditionGuid, src.Code, src.Name, CURRENT_TIMESTAMP(), TRUE);

-- COMMAND ----------

-- DBTITLE 1,d_smoking_status
MERGE INTO ey_team_40_gold.d_smoking_status tgt
USING ey_team_40_silver.smoking_status src
ON tgt.SmokingStatusGuid = src.SmokingStatusGuid
WHEN MATCHED AND (tgt.Description <> src.Description OR
                  tgt.NISTcode <> src.NISTcode )
THEN
  UPDATE SET tgt.CurrentFlag = FALSE -- Mark the existing record's CurrentFlag as False
WHEN NOT MATCHED THEN
  INSERT (SmokingStatusGuid,Description,NISTcode,LoadDate,CurrentFlag)
  VALUES (src.SmokingStatusGuid, src.Description, src.NISTcode, CURRENT_TIMESTAMP(), TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fact tables

-- COMMAND ----------

-- DBTITLE 1,f_Patients_Condition
CREATE OR REPLACE TABLE ey_team_40_gold.f_Patients_Condition
AS
(SELECT 
  f.PatientConditionGuid
  ,f.PatientGuid
  ,f.ConditionGuid
  ,f.CreatedYear
  ,p.Gender
  ,p.YearOfBirth
  ,(2024-p.YearOfBirth) as Age
  ,p.State
  ,c.Code
  ,c.Name as ConditionName
  ,CURRENT_TIMESTAMP() as LoadDate
FROM ey_team_40_silver.patient_condition f
  LEFT JOIN ey_team_40_gold.d_patients p
    ON f.PatientGuid = p.PatientGuid
LEFT JOIN ey_team_40_gold.d_medical_condition c
    ON f.ConditionGuid = c.ConditionGuid)

-- COMMAND ----------

-- DBTITLE 1,f_Patients_SmokingStatus
CREATE OR REPLACE TABLE ey_team_40_gold.f_Patients_SmokingStatus
AS
(SELECT 
  f.PatientSmokingStatusGuid
  ,f.PatientGuid
  ,f.SmokingStatusGuid
  ,f.EffectiveYear
  ,p.Gender
  ,p.YearOfBirth
  ,(2024-p.YearOfBirth) as Age
  ,p.State
  ,s.Description as SmokingStatus
  ,CURRENT_TIMESTAMP() as LoadDate
FROM ey_team_40_bronze.PatientSmokingStatus_Raw f
  LEFT JOIN ey_team_40_gold.d_patients p
    ON f.PatientGuid = p.PatientGuid
LEFT JOIN ey_team_40_gold.d_smoking_status s
    ON f.SmokingStatusGuid = s.SmokingStatusGuid)

-- COMMAND ----------

SHOW TABLES IN ey_team_40_gold

-- COMMAND ----------

-- DBTITLE 1,Example metrics to be implemented in dashboard
/*CREATE OR REPLACE TABLE  ey_team_40_gold.Patients_Condition_by_YearOfBirth
COMMENT "This a Metric for Patient's Condition by YearOfBirth for Patient-360 app"
AS
(SELECT  YearOfBirth,ConditionName,count(patientguid) as Total_Patients
FROM ey_team_40_silver.Patients_Condition_cleaned
GROUP BY YearOfBirth,ConditionName
ORDER BY YearOfBirth,ConditionName)*/

-- COMMAND ----------

/*%sql
CREATE OR REPLACE TABLE  ey_team_40_gold.Patients_Condition_by_Age
COMMENT "This a Metric for Patient's Condition by YearOfBirth for Patient-360 app"
AS
(SELECT  Age,ConditionName,count(patientguid) as Total_Patients
FROM ey_team_40_silver.Patients_Condition_cleaned
GROUP BY Age,ConditionName
ORDER BY Age,ConditionName)*/

-- COMMAND ----------

/*%sql
CREATE OR REPLACE TABLE  ey_team_40_gold.Patients_SmokingStatus_by_Age
COMMENT "This a Metric for Patient's Smoking Status by Age for Patient-360 app"
AS
(SELECT  Age,SmokingStatus,count(patientguid) as Total_Patients
FROM ey_team_40_silver.Patients_SmokingStatus_cleaned
GROUP BY Age,SmokingStatus
ORDER BY Age,SmokingStatus)*/


