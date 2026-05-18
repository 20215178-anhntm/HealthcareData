from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType)

class HealthcareSchemas:
    @staticmethod
    def patients_schema() -> StructType:
        return StructType([
            StructField("Name", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Blood Type", StringType(), True),
            StructField("Medical Condition", StringType(), True),
            StructField("Date of Admission", DateType(), True),
            StructField("Doctor", StringType(), True),
            StructField("Hospital", StringType(), True),
            StructField("Insurance Provider", StringType(), True),
            StructField("Billing Amount", DoubleType(), True),
            StructField("Room Number", IntegerType(), True),
            StructField("Admission Type", StringType(), True),
            StructField("Discharge Date", DateType(), True),
            StructField("Medication", StringType(), True),
            StructField("Test Results", StringType(), True),
        ])
    
    @staticmethod
    def insurance_schema() -> StructType:
        return StructType([
            StructField("age", IntegerType(), True),
            StructField("sex", StringType(), True),
            StructField("bmi", DoubleType(), True),
            StructField("children", IntegerType(), True),
            StructField("smoker", StringType(), True),
            StructField("region", StringType(), True),
            StructField("charges", DoubleType(), True),
        ])
    
    @staticmethod
    def diabetes_schema() -> StructType:
        return StructType([
            StructField("Id", IntegerType(), True),
            StructField("Pregnancies", IntegerType(), True),
            StructField("Glucose", IntegerType(), True),
            StructField("BloodPressure", IntegerType(), True),
            StructField("SkinThickness", IntegerType(), True),
            StructField("Insulin", IntegerType(), True),
            StructField("BMI", DoubleType(), True),
            StructField("DiabetesPedigreeFunction", DoubleType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Outcome", IntegerType(), True),
        ])
    
    @staticmethod
    def noshows_schema() -> StructType:
        return StructType([
            StructField("PatientId", DoubleType(), True), 
            StructField("AppointmentID", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("ScheduledDay", DateType(), True),
            StructField("AppointmentDay", DateType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Neighbourhood", StringType(), True),
            StructField("Scholarship", BooleanType(), True),
            StructField("Hipertension", BooleanType(), True),
            StructField("Diabetes", BooleanType(), True),
            StructField("Alcoholism", BooleanType(), True),
            StructField("Handcap", BooleanType(), True),
            StructField("SMS_received", BooleanType(), True),
            StructField("Showed_up", BooleanType(), True),
            StructField("Date.diff", IntegerType(), True),  
        ])