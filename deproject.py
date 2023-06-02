from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

import pandas as pd
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
import numpy as np
import os, io
import boto3
import json
import requests
import pickle
from bs4 import BeautifulSoup
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        FloatType,
    )
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, count, avg, trim, expr, mean, log, lit
from pyspark.ml import Pipeline as PipelineSpark
from pyspark.ml.classification import (
    LogisticRegression as LogisticRegressionSpark,
    RandomForestClassifier,
    GBTClassifier,
    LinearSVC,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler as MinMaxScalerSpark, Imputer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, label_binarize
from sklearn.compose import make_column_transformer
from sklearn.svm import SVC
from sklearn.svm import LinearSVC as LinearSVCSklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
import csv
from io import StringIO

WORKFLOW_SCHEDULE_INTERVAL = None

hook = S3Hook(aws_conn_id='aws_default')

bucket_name = "group11-data"
S3_SOURCE_DATA_PATH = "s3://group11-data/source_data/"
S3_OUT_DATA_PATH = "s3://de300-group11-mwaa-output/sprk_data.csv"
heart_data_path = S3_SOURCE_DATA_PATH + "heart_disease.csv"
# out_text_path = "s3://group11-data/output.txt"

# Define the default args dictionary
default_args = {
    'owner': 'willxenakis',
    'depends_on_past': False,
    'start_date': days_ago(1), # should be a historical date
    'retries': 1,
}

# Define the schema for your data
schema = StructType([
    StructField("age", DoubleType(), True),
    StructField("sex", DoubleType(), True),
    StructField("painloc", DoubleType(), True),
    StructField("painexer", DoubleType(), True),
    StructField("relrest", DoubleType(), True),
    StructField("pncaden", DoubleType(), True),
    StructField("cp", DoubleType(), True),
    StructField("trestbps", DoubleType(), True),
    StructField("htn", DoubleType(), True),
    StructField("chol", DoubleType(), True),
    StructField("smoke", DoubleType(), True),
    StructField("cigs", DoubleType(), True),
    StructField("years", DoubleType(), True),
    StructField("fbs", DoubleType(), True),
    StructField("dm", DoubleType(), True),
    StructField("famhist", DoubleType(), True),
    StructField("restecg", DoubleType(), True),
    StructField("ekgmo", DoubleType(), True),
    StructField("ekgday", DoubleType(), True),
    StructField("ekgyr", DoubleType(), True),
    StructField("dig", DoubleType(), True),
    StructField("prop", DoubleType(), True),
    StructField("nitr", DoubleType(), True),
    StructField("pro", DoubleType(), True),
    StructField("diuretic", DoubleType(), True),
    StructField("proto", DoubleType(), True),
    StructField("thaldur", DoubleType(), True),
    StructField("thaltime", DoubleType(), True),
    StructField("met", DoubleType(), True),
    StructField("thalach", DoubleType(), True),
    StructField("thalrest", DoubleType(), True),
    StructField("tpeakbps", DoubleType(), True),
    StructField("tpeakbpd", DoubleType(), True),
    StructField("dummy", DoubleType(), True),
    StructField("trestbpd", DoubleType(), True),
    StructField("exang", DoubleType(), True),
    StructField("xhypo", DoubleType(), True),
    StructField("oldpeak", DoubleType(), True),
    StructField("slope", DoubleType(), True),
    StructField("rldv5", DoubleType(), True),
    StructField("rldv5e", DoubleType(), True),
    StructField("ca", DoubleType(), True),
    StructField("restckm", DoubleType(), True),
    StructField("exerckm", DoubleType(), True),
    StructField("restef", DoubleType(), True),
    StructField("restwm", DoubleType(), True),
    StructField("exeref", DoubleType(), True),
    StructField("exerwm", DoubleType(), True),
    StructField("thal", DoubleType(), True),
    StructField("thalsev", DoubleType(), True),
    StructField("thalpul", DoubleType(), True),
    StructField("earlobe", DoubleType(), True),
    StructField("cmo", DoubleType(), True),
    StructField("cday", DoubleType(), True),
    StructField("cyr", DoubleType(), True),
    StructField("target", DoubleType(), True),
])

limited_scheme = StructType([
    StructField("age", DoubleType(), True),
    StructField("sex", DoubleType(), True),
    StructField("painloc", DoubleType(), True),
    StructField("painexer", DoubleType(), True),
    StructField("cp", DoubleType(), True),
    StructField("trestbps", DoubleType(), True),
    StructField("smoke", DoubleType(), True),
    StructField("fbs", DoubleType(), True),
    StructField("prop", DoubleType(), True),
    StructField("nitr", DoubleType(), True),
    StructField("pro", DoubleType(), True),
    StructField("diuretic", DoubleType(), True),
    StructField("thaldur", DoubleType(), True),
    StructField("thalach", DoubleType(), True),
    StructField("exang", DoubleType(), True),
    StructField("oldpeak", DoubleType(), True),
    StructField("slope", DoubleType(), True),
    StructField("target", DoubleType(), True),
])

limited_scheme_fe = StructType([
    StructField("age", DoubleType(), True),
    StructField("sex", DoubleType(), True),
    StructField("painloc", DoubleType(), True),
    StructField("painexer", DoubleType(), True),
    StructField("cp", DoubleType(), True),
    StructField("trestbps", DoubleType(), True),
    StructField("smoke", DoubleType(), True),
    StructField("fbs", DoubleType(), True),
    StructField("prop", DoubleType(), True),
    StructField("nitr", DoubleType(), True),
    StructField("pro", DoubleType(), True),
    StructField("diuretic", DoubleType(), True),
    StructField("thaldur", DoubleType(), True),
    StructField("thalach", DoubleType(), True),
    StructField("exang", DoubleType(), True),
    StructField("oldpeak", DoubleType(), True),
    StructField("slope", DoubleType(), True),
    StructField("target", DoubleType(), True),
    StructField("trestbpsxthalach", DoubleType(), True),
])

limited_scheme_merged = StructType([
    StructField("age", DoubleType(), True),
    StructField("sex", DoubleType(), True),
    StructField("painloc", DoubleType(), True),
    StructField("painexer", DoubleType(), True),
    StructField("cp", DoubleType(), True),
    StructField("trestbps", DoubleType(), True),
    StructField("smoke", DoubleType(), True),
    StructField("fbs", DoubleType(), True),
    StructField("prop", DoubleType(), True),
    StructField("nitr", DoubleType(), True),
    StructField("pro", DoubleType(), True),
    StructField("diuretic", DoubleType(), True),
    StructField("thaldur", DoubleType(), True),
    StructField("thalach", DoubleType(), True),
    StructField("exang", DoubleType(), True),
    StructField("oldpeak", DoubleType(), True),
    StructField("slope", DoubleType(), True),
    StructField("target", DoubleType(), True),
    StructField("trestbpsxthalach", DoubleType(), True),
    StructField("smoke_source1", DoubleType(), True),
    StructField("smoke_source2", DoubleType(), True),

])
				
def upload_to_s3(bucket:str, key:str, file_path:str):
    s3_client = boto3.client("s3")
    s3_client.to_csv("/tmp/wine_dataset.csv", index=False)

def download_from_s3(bucket: str, key: str, file_path: str):
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, file_path)

def read_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def load_data_func(**kwargs):
    print('Running Load Data')

    print(f'Load Data Func: success')


    return {'status': 1}

def sklrn_cln_impute_func(**kwargs):
    print('Running SKLearn Clean & Impute')
    # Visually you can see which columns are categorial and which are numerical
    categorical = ['htn','restecg','dig','nitr','pro','diuretic','exang','xhypo']
    full_categorical = ['sex', 'htn','restecg','dig','nitr','prop','pro','diuretic','exang','xhypo', 'target']
    numerical = ['trestbps','chol','ekgmo','ekgday','ekgyr','thaldur','thalach','thalrest','tpeakbps','tpeakbpd','dummy','trestbpd','oldpeak','cmo','cday','cyr']

    s3_key = 'source_data/heart_disease.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="group11-data")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8').head(899)

    pandas_df = pandas_df[full_categorical + numerical]

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Calculate the percentage of missing values for each column
    null_percentages = pandas_df.isnull().sum() / pandas_df.shape[0]

    # Get the columns to drop based on the threshold (7%)
    columns_to_drop = null_percentages[null_percentages > 0.12].index

    for dropped in columns_to_drop:
        if dropped in full_categorical:
            full_categorical.remove(dropped)
        elif dropped in numerical:
            numerical.remove(dropped)

    if "target" in columns_to_drop: 
        columns_to_drop.remove("target")

    # Drop the columns from the raw data
    cleaned_data = pandas_df.drop(columns=columns_to_drop)

    print(cleaned_data.columns)

    # Define the column transformer to handle missing values and drop columns
    ct = ColumnTransformer(
        transformers=[
            ("imputer_median", SimpleImputer(strategy="median"), numerical),
            ("imputer_mode", SimpleImputer(strategy="most_frequent"), full_categorical)
        ],
        remainder="passthrough"
    )

    # Fit and transform the data
    cleaned_data = ct.fit_transform(cleaned_data)

    # Create a new DataFrame with the cleaned data
    cleaned_df = pd.DataFrame(cleaned_data, columns=numerical+full_categorical)

    # Standardize the numerical columns
    scaler = StandardScaler()
    cleaned_df[numerical] = scaler.fit_transform(cleaned_df[numerical])

    # Plot histograms using matplotlib
    fig, ax = plt.subplots(nrows=6, ncols=5, figsize=(15, 15))
    for index, col in enumerate(cleaned_df.columns):
        if col == "target_column": 
            continue
        row, column = divmod(index, 5)
        ax[row, column].hist(cleaned_df[col], bins=20)
        ax[row, column].set_title(col)

    plt.tight_layout()
    plt.savefig("/usr/local/airflow/cleaned_data_histogram.png")

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklearn_cleaned_data_histogram.png"

    s3_client = boto3.client('s3')
    s3_client.upload_file("/usr/local/airflow/cleaned_data_histogram.png", bucket_name, key)

    # Identify and remove outliers using Isolation Forest
    outlier_detector = IsolationForest(contamination=0.05)
    outlier_detector.fit(cleaned_df[numerical])

    no_outliers = cleaned_df[outlier_detector.predict(cleaned_df[numerical]) == 1]

    no_outliers = no_outliers[no_outliers['prop'] != 22]
    no_outliers = no_outliers[~no_outliers['cyr'].isin([1,26,16])]
    no_outliers = no_outliers[no_outliers['dummy'] != 0]
    no_outliers = no_outliers[no_outliers['trestbps'] != 0]
    no_outliers = no_outliers[no_outliers['tpeakbpd'] != 11]
    no_outliers = no_outliers[no_outliers['trestbpd'] != 0]

    file_path = "/usr/local/airflow/sklearn_clnd_impt_data.csv"
    no_outliers.to_csv(file_path, index=False)

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklrn_clnd_impt_data.csv"

    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, bucket_name, key)

    print(f'SKLearn Clean & Impute output: {1}')

    return {'value': 1}

def sklrn_fe_func(**kwargs):
    print('Running SKLearn Feature Engineering')
    # Create a column transformer for normalizing numerical columns
    full_categorical = ['sex', 'htn','restecg','dig','nitr','prop','pro','diuretic','exang','xhypo']
    numerical = ['trestbps','chol','ekgmo','ekgday','ekgyr','thaldur','thalach','thalrest','tpeakbps','tpeakbpd','dummy','trestbpd','oldpeak','cmo','cday','cyr']

    s3_key = 'sklrn_clnd_impt_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    no_outliers = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    numeric_transformer = Pipeline(steps=[
        ('scaler', MinMaxScaler())
    ])

    # cols that needs transform
    totransform = ['thaldur', 'thalrest', 'oldpeak']

    # Transform the numerical columns
    no_outliers[totransform] = numeric_transformer.fit_transform(no_outliers[totransform])

    # One-hot encode categorical columns
    no_outliers = pd.get_dummies(no_outliers, columns=full_categorical)

    # Standardize the numerical columns
    scaler = StandardScaler()
    no_outliers[numerical] = scaler.fit_transform(no_outliers[numerical])

    # Plot histograms using matplotlib
    fig, ax = plt.subplots(nrows=8, ncols=8, figsize=(15, 15))
    for index, col in enumerate(no_outliers.columns):
        if col == "target_column": 
            continue
        row, column = divmod(index, 8)
        ax[row, column].hist(no_outliers[col], bins=20)
        ax[row, column].set_title(col)

    plt.tight_layout()
    plt.savefig("/usr/local/airflow/histograms_post_transformations.png")

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklearn_histograms_post_transformations.png"

    s3_client = boto3.client('s3')
    s3_client.upload_file("/usr/local/airflow/histograms_post_transformations.png", bucket_name, key)

    file_path = "/usr/local/airflow/sklearn_fe_data.csv"
    no_outliers.to_csv(file_path, index=False)

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklearn_fe_data.csv"

    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, bucket_name, key)

    return {'value': 1}

def sklrn_lr_func(**kwargs):
    print('Running SKLearn LR')
    s3_key = 'sklearn_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    model_data = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Get a list of all column names
    column_names = list(model_data.columns)

    # Remove the "target" column if present
    if "target" in column_names:
        column_names.remove("target")


    # Step 3: Prepare your data
    X = model_data[column_names]  # Features (independent variables)
    y = model_data['target']  # Target variable (dependent variable)

    # Step 4: Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)

    # Step 5: Create an instance of LogisticRegression
    model = LogisticRegression()

    # Step 6: Fit the logistic regression model to the training data
    model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_test)[:, 1]  # Predicted probabilities for class 1
    y_pred = model.predict(X_test)

    # Step 8: Evaluate the performance of the model
    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)
    print('Accuracy:', accuracy)
    print('ROC AUC:', roc_auc)

    serialized_model = pickle.dumps(model)


    # Save the serialized model to a file
    with open('/usr/local/airflow/sklearn_lr_model.pkl', 'wb') as f:
        f.write(serialized_model)

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklearn_lr_model.pkl"

    s3_client = boto3.client('s3')
    s3_client.upload_file("/usr/local/airflow/sklearn_lr_model.pkl", bucket_name, key)

    return {'rocauc': roc_auc}

def sklrn_svm_func(**kwargs):
    print('Running SKLearn SVM')
    
    s3_key = 'sklearn_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    model_data = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Get a list of all column names
    column_names = list(model_data.columns)

    # Remove the "target" column if present
    if "target" in column_names:
        column_names.remove("target")

    # Step 3: Prepare your data
    X = model_data[column_names]  # Features (independent variables)
    y = model_data['target']  # Target variable (dependent variable)

    # Step 4: Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)

    # Step 5: Create an instance of LinearSVC
    model = LinearSVCSklearn(C=1.0)

    # Step 6: Fit the LinearSVC model to the training data
    model.fit(X_train, y_train)

    # Step 7: Predict the target variable for the testing data
    y_pred = model.predict(X_test)

    # Step 8: Evaluate the performance of the model
    accuracy = accuracy_score(y_test, y_pred)

    # Step 7: Obtain the decision function scores for the testing data
    decision_scores = model.decision_function(X_test)

    # Step 8: Convert the target variable to binary form
    y_test_binary = label_binarize(y_test, classes=model.classes_)

    # Step 9: Calculate the ROC AUC score
    roc_auc = roc_auc_score(y_test_binary, decision_scores)
    print('ROC AUC:', roc_auc)
    print('Accuracy:', accuracy)

    serialized_model = pickle.dumps(model)

    # Save the serialized model to a file
    with open('/usr/local/airflow/sklearn_svm_model.pkl', 'wb') as f:
        f.write(serialized_model)

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sklearn_svm_model.pkl"

    s3_client = boto3.client('s3')
    s3_client.upload_file("/usr/local/airflow/sklearn_svm_model.pkl", bucket_name, key)

    return {'rocauc': roc_auc}

def sprk_cln_impute_func(**kwargs):
    print('Running Spark Clean & Impute')
    
    spark = SparkSession.builder.getOrCreate()

    s3_key = 'source_data/heart_disease.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="group11-data")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8').head(899)

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, schema)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")

    selected_columns = [
    "age",
    "sex",
    "painloc",
    "painexer",
    "cp",
    "trestbps",
    "smoke",
    "fbs",
    "prop",
    "nitr",
    "pro",
    "diuretic",
    "thaldur",
    "thalach",
    "exang",
    "oldpeak",
    "slope",
    "target",
    ]
    data_selected = data.select(*selected_columns)

    # Calculate the mode of 'painloc' and 'painexer' columns
    mode_painloc = (
        data_selected.groupBy("painloc")
        .count()
        .orderBy("count", ascending=False)
        .first()[0]
    )
    mode_painexer = (
        data_selected.groupBy("painexer")
        .count()
        .orderBy("count", ascending=False)
        .first()[0]
    )

    # Replace missing values in 'painloc' column with the mode value
    data_selected = data_selected.fillna({"painloc": mode_painloc})

    # Replace missing values in 'painexer' column with the mode value
    data_selected = data_selected.fillna({"painexer": mode_painexer})

    # Calculate the mean of 'trestbps' column
    mean_trestbps = data_selected.select(mean("trestbps")).collect()[0][0]

    # Replace values less than 100 mm Hg with the mean value
    data_selected = data_selected.withColumn(
        "trestbps", when(col("trestbps") < 100, mean_trestbps).otherwise(col("trestbps"))
    )

    # Replace missing values in 'trestbps' column with the mean value
    data_selected = data_selected.fillna({"trestbps": mean_trestbps})

    data_selected = data_selected.withColumn("trestbps", \
        when(col("trestbps")=="" ,mean_trestbps) \
            .otherwise(col("trestbps"))) \
    # Calculate the mean of 'oldpeak' column
    mean_oldpeak = data_selected.select(mean("oldpeak")).collect()[0][0]

    # Replace values less than 0 and greater than 4 with the mean value
    data_selected = data_selected.withColumn(
        "oldpeak",
        when((col("oldpeak") < 0) | (col("oldpeak") > 4), mean_oldpeak).otherwise(
            col("oldpeak")
        ),
    )

    # Replace missing values in 'oldpeak' column with the mean value
    data_selected = data_selected.fillna({"oldpeak": mean_oldpeak})

    # data_selected = data_selected.withColumn("oldpeak", col("oldpeak").fillna(mean_oldpeak))

    # Calculate the mean of 'thaldur' column
    mean_thaldur = data_selected.select(mean("thaldur")).collect()[0][0]

    # Calculate the mean of 'thalach' column
    mean_thalach = data_selected.select(mean("thalach")).collect()[0][0]

    # Replace missing values in 'thaldur' column with the mean value
    data_selected = data_selected.fillna({"thaldur": mean_thaldur})

    # data_selected = data_selected.withColumn("thaldur", col("thaldur").fillna(mean_thaldur))

    # Replace missing values in 'thalach' column with the mean value
    data_selected = data_selected.fillna({"thalach": mean_thalach})

    # data_selected = data_selected.withColumn("thalach", col("thalach").fillna(mean_thalach))

    input_cols = ['trestbps', 'thaldur', 'thalach', 'oldpeak']

    # Create an instance of the Imputer class
    imputer = Imputer(
        inputCols=input_cols,
        outputCols=input_cols,
        strategy='mean'  # Specify the imputation strategy here, e.g., 'mean', 'median', 'mode', or a specific constant value
    )

    # Fit the Imputer to the DataFrame to compute the imputation values
    imputer_model = imputer.fit(data_selected)

    # Apply the imputation to the DataFrame
    data_selected = imputer_model.transform(data_selected)

    # Calculate the mode of 'fbs' column
    mode_fbs = (
        data_selected.groupBy("fbs").count().orderBy("count", ascending=False).first()[0]
    )

    # Calculate the mode of 'prop' column
    mode_prop = (
        data_selected.groupBy("prop").count().orderBy("count", ascending=False).first()[0]
    )

    # Calculate the mode of 'nitr' column
    mode_nitr = (
        data_selected.groupBy("nitr").count().orderBy("count", ascending=False).first()[0]
    )

    # Calculate the mode of 'pro' column
    mode_pro = (
        data_selected.groupBy("pro").count().orderBy("count", ascending=False).first()[0]
    )

    # Calculate the mode of 'diuretic' column
    mode_diuretic = (
        data_selected.groupBy("diuretic")
        .count()
        .orderBy("count", ascending=False)
        .first()[0]
    )

    # Replace missing values and values greater than 1 in 'fbs' column with the mode value
    data_selected = data_selected.withColumn(
        "fbs",
        when((col("fbs").isNull()) | (col("fbs") > 1), mode_fbs).otherwise(col("fbs")),
    )

    # Replace missing values and values greater than 1 in 'prop' column with the mode value
    data_selected = data_selected.withColumn(
        "prop",
        when((col("prop").isNull()) | (col("prop") > 1), mode_prop).otherwise(col("prop")),
    )

    # Replace missing values and values greater than 1 in 'nitr' column with the mode value
    data_selected = data_selected.withColumn(
        "nitr",
        when((col("nitr").isNull()) | (col("nitr") > 1), mode_nitr).otherwise(col("nitr")),
    )

    # Replace missing values and values greater than 1 in 'pro' column with the mode value
    data_selected = data_selected.withColumn(
        "pro",
        when((col("pro").isNull()) | (col("pro") > 1), mode_pro).otherwise(col("pro")),
    )

    # Replace missing values and values greater than 1 in 'diuretic' column with the mode value
    data_selected = data_selected.withColumn(
        "diuretic",
        when((col("diuretic").isNull()) | (col("diuretic") > 1), mode_diuretic).otherwise(
            col("diuretic")
        ),
    )

    data_selected.show()

    # Calculate the mode of 'exang' column
    mode_exang = (
        data_selected.groupBy("exang").count().orderBy("count", ascending=False).first()[0]
    )

    # Calculate the mode of 'slope' column
    mode_slope = (
        data_selected.groupBy("slope").count().orderBy("count", ascending=False).first()[0]
    )

    # Replace missing values in 'exang' column with the mode value
    data_selected = data_selected.fillna({"exang": mode_exang})

    # Replace missing values in 'slope' column with the mode value
    data_selected = data_selected.fillna({"slope": mode_slope})

    print(f'Spark Clean & Impute: {1} {1}')

    
    data_selected = data_selected.na.drop("all")

    # Code to load data back to s3 is from ChatGPT

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df_end = data_selected.toPandas()

    print(pandas_df_end.isnull().sum())

    file_path = "/usr/local/airflow/sprk_clnd_impt_data.csv"
    pandas_df_end.to_csv(file_path, index=False)

    # Upload the CSV file to S3 using boto3
    bucket_name = "de300-group11-mwaa-output"
    key = "sprk_clnd_impt_data.csv"

    s3_client = boto3.client('s3')
    s3_client.upload_file(file_path, bucket_name, key)

    # # Save the Pandas DataFrame to a CSV file
    # csv_data = pandas_df_end.to_csv(index=False)

    # # Write the CSV data to S3 using s3_hook
    # hook.load_string(csv_data, key="sprk_clnd_impt_data.csv", bucket_name="de300-group11-mwaa-output", replace=True)

    return {'value1': 1, 'value2': 1}

def sprk_fe_func(**kwargs):
    print('Running Spark Feature Engineering')
    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_clnd_impt_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")


    # Do some feature engineering here:
    # Putting trestbps and thalach together makes sense because the former is resting blood pressure and the latter is maximum heart rate achieved
    data = data.withColumn("trestbpsxthalach", col("trestbps")* col("thalach"))

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = data.toPandas()

    # Save the Pandas DataFrame to a CSV file
    csv_data = pandas_df.to_csv(index=False)

    # Write the CSV data to S3 using s3_hook
    hook.load_string(csv_data, key="sprk_fe_data.csv", bucket_name="de300-group11-mwaa-output", replace=True)

    print(f'Spark FE: {1} {1}')

    return {'value1': 1, 'value2': 1}

def sprk_lr_func(**kwargs):
    print('Running Spark LR')
    
    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme_fe)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")

    # Drop smoke column
    data = data.drop("smoke")

    # Convert the target column to numeric if it's not already
    if "target" not in data.columns:
        raise ValueError("The DataFrame does not contain a 'target' column.")
    target_indexer = StringIndexer(inputCol="target", outputCol="label")
    df_labeled = target_indexer.fit(data).transform(data)

    # Calculate the proportions of positive and negative labels
    label_counts = df_labeled.groupBy("label").count().collect()
    positive_label_count = label_counts[1][1]
    total_count = df_labeled.count()

    # Calculate the proportions for stratification
    positive_proportion = positive_label_count / total_count
    negative_proportion = 1 - positive_proportion

    # Calculate the desired number of samples for each class
    train_positive_count = int(0.9 * positive_label_count)
    train_negative_count = int(0.9 * (total_count - positive_label_count))

    # Perform the stratified split with the desired counts
    train_data = df_labeled.sampleBy(
        "label",
        fractions={
            0: train_negative_count / (total_count - positive_label_count),
            1: train_positive_count / positive_label_count,
        },
        seed=42,
    )
    test_data = df_labeled.subtract(train_data)

    # Verify the proportions of positive labels in both sets
    train_positive_count = train_data.filter(col("label") == 1).count()
    test_positive_count = test_data.filter(col("label") == 1).count()

    train_positive_proportion = train_positive_count / train_data.count()
    test_positive_proportion = test_positive_count / test_data.count()

    print(
        f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
    )
    print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

    train_data.count()

    test_data.count()

    # Assuming you have already split the data into train_data and test_data

    # Define the feature columns
    feature_columns = [
        "age",
        "sex",
        "painloc",
        "painexer",
        "cp",
        "trestbps",
        "fbs",
        "prop",
        "nitr",
        "pro",
        "diuretic",
        "thaldur",
        "thalach",
        "exang",
        "oldpeak",
        "slope",
        "trestbpsxthalach",
    ]

    # Define the target column
    target_column = "target"

    # Convert string columns to appropriate data types
    for column in feature_columns + [target_column]:
        train_data = train_data.withColumn(column, train_data[column].cast("double"))

    # Convert the test data columns to appropriate data types
    for column in feature_columns + [target_column]:
        test_data = test_data.withColumn(column, test_data[column].cast("double"))

    # Create the vector assembler
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

    # Apply MinMaxScaling transformation
    scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

    # Transform the training data
    train_data = assembler.transform(train_data)

    train_data = scaler.fit(train_data).transform(train_data)

    # Define the classification models
    logistic_regression = LogisticRegressionSpark(featuresCol="features", labelCol=target_column)
    # logistic_regression = LogisticRegressionSp(labelCol=target_column)

    # Define the parameter grids for hyperparameter tuning
    param_grid_lr = (
        ParamGridBuilder().addGrid(logistic_regression.regParam, [0.01, 0.1]).build()
    )

    # Create the pipeline for each model
    pipeline_lr = PipelineSpark(stages=[logistic_regression])
    
    # Create the evaluator for binary classification
    evaluator = BinaryClassificationEvaluator(labelCol=target_column)

    # Perform cross-validation and hyperparameter tuning for each model
    cv_lr = CrossValidator(
        estimator=pipeline_lr,
        estimatorParamMaps=param_grid_lr,
        evaluator=evaluator,
        numFolds=5,
    )

    # Fit the models to the training data
    cv_model_lr = cv_lr.fit(train_data)

    # Transform the test data
    test_data = assembler.transform(test_data)

    test_data = scaler.fit(test_data).transform(test_data)

    # Evaluate the models on the test data
    result_lr = cv_model_lr.transform(test_data)

    # Evaluate performance metrics
    auc_lr = evaluator.evaluate(result_lr)

    print(auc_lr)

    # Get the best model for logistic regression
    best_model_lr = cv_model_lr.bestModel
    # Extract the best hyperparameters
    best_params_lr = best_model_lr.stages[0].extractParamMap()
    best_reg_param = best_params_lr[logistic_regression.regParam]
    # Print the best hyperparameters
    print("Best Hyperparameters for Logistic Regression:")
    print(f"Regularization Parameter: {best_reg_param}")

    return {'rocauc': auc_lr}

def sprk_svm_func(**kwargs):
    print('Running Spark SVM')

    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme_fe)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")

    # Drop smoke column
    data = data.drop("smoke")

    # Convert the target column to numeric if it's not already
    if "target" not in data.columns:
        raise ValueError("The DataFrame does not contain a 'target' column.")
    target_indexer = StringIndexer(inputCol="target", outputCol="label")
    df_labeled = target_indexer.fit(data).transform(data)

    # Calculate the proportions of positive and negative labels
    label_counts = df_labeled.groupBy("label").count().collect()
    positive_label_count = label_counts[1][1]
    total_count = df_labeled.count()

    # Calculate the proportions for stratification
    positive_proportion = positive_label_count / total_count
    negative_proportion = 1 - positive_proportion

    # Calculate the desired number of samples for each class
    train_positive_count = int(0.9 * positive_label_count)
    train_negative_count = int(0.9 * (total_count - positive_label_count))

    # Perform the stratified split with the desired counts
    train_data_svm = df_labeled.sampleBy(
        "label",
        fractions={
            0: train_negative_count / (total_count - positive_label_count),
            1: train_positive_count / positive_label_count,
        },
        seed=42,
    )
    test_data_svm = df_labeled.subtract(train_data_svm)

    # Verify the proportions of positive labels in both sets
    train_positive_count = train_data_svm.filter(col("label") == 1).count()
    test_positive_count = test_data_svm.filter(col("label") == 1).count()

    train_positive_proportion = train_positive_count / train_data_svm.count()
    test_positive_proportion = test_positive_count / test_data_svm.count()

    print(
        f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
    )
    print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

    train_data_svm.count()

    test_data_svm.count()

    # Assuming you have already split the data into train_data_svm and test_data_svm

    # Define the feature columns
    feature_columns = [
        "age",
        "sex",
        "painloc",
        "painexer",
        "cp",
        "trestbps",
        "fbs",
        "prop",
        "nitr",
        "pro",
        "diuretic",
        "thaldur",
        "thalach",
        "exang",
        "oldpeak",
        "slope",
        "trestbpsxthalach",
    ]

    # Define the target column
    target_column = "target"

    # Convert string columns to appropriate data types
    for column in feature_columns + [target_column]:
        train_data_svm = train_data_svm.withColumn(column, train_data_svm[column].cast("double"))

    # Convert the test data columns to appropriate data types
    for column in feature_columns + [target_column]:
        test_data_svm = test_data_svm.withColumn(column, test_data_svm[column].cast("double"))

    # Create the vector assembler
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

    # Apply MinMaxScaling transformation
    scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

    # Transform the training data
    train_data_svm = assembler.transform(train_data_svm)

    train_data_svm = scaler.fit(train_data_svm).transform(train_data_svm)

    # Define the classification models
    svm = LinearSVC(featuresCol="features", labelCol=target_column)
    # logistic_regression = LogisticRegressionSp(labelCol=target_column)

    # Define the parameter grids for hyperparameter tuning
    # Define the parameter grid for hyperparameter tuning
    param_grid_svm = (
        ParamGridBuilder()
        .addGrid(svm.regParam, [0.01, 0.1])
        .addGrid(svm.maxIter, [10, 100])
        .build()
    )

    # Create the pipeline for the SVM model
    pipeline_svm = PipelineSpark(stages=[svm])
    
    # Create the evaluator for binary classification
    evaluator = BinaryClassificationEvaluator(labelCol=target_column)

    # Perform cross-validation and hyperparameter tuning for the SVM model
    cv_svm = CrossValidator(
        estimator=pipeline_svm,
        estimatorParamMaps=param_grid_svm,
        evaluator=evaluator,
        numFolds=5,
    )

    # Fit the SVM model to the training data
    cv_model_svm = cv_svm.fit(train_data_svm)

    # Transform the test data for the SVM model
    test_data_svm = assembler.transform(test_data_svm)
    test_data_svm = scaler.fit(test_data_svm).transform(test_data_svm)

    # Evaluate the SVM model on the test data
    result_svm = cv_model_svm.transform(test_data_svm)

    # Evaluate performance metrics for the SVM model
    auc_svm = evaluator.evaluate(result_svm)
    print(auc_svm)

    # Get the best model for the SVM model
    best_model_svm = cv_model_svm.bestModel
    # Extract the best hyperparameters for the SVM model
    best_params_svm = best_model_svm.stages[0].extractParamMap()
    best_reg_param_svm = best_params_svm[svm.regParam]
    best_max_iter_svm = best_params_svm[svm.maxIter]

    # Print the best hyperparameters for the SVM model
    print("Best Hyperparameters for SVM:")
    print(f"Regularization Parameter: {best_reg_param_svm}")
    print(f"Maximum Iterations: {best_max_iter_svm}")
    return {'rocauc': auc_svm}

def scrape_func(**kwargs):
    print('Running Data Scraping')
    # Returns two random integers between 0 and 10
    # URL of the website to scrape
    url = "https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/2020-21"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    data_elements = soup.find_all("tr", class_="even")
    source1_data = []
    for i in range(7):
        cur = []
        if i == 6:
            # 75+
            cur.append(75)
            cur.append(200)
        else:
            agetxt = data_elements[i].select("th")[0].text
            dash = agetxt.index(chr(8211))
            cur.append(int(agetxt[dash - 2 : dash]))
            cur.append(int(agetxt[dash + 1 : dash + 3]))
        cur.append(float(data_elements[i].select("td")[0].text))
        source1_data.append(cur)
    print(source1_data)

    # URL of the website to scrape
    url = "https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    data_elements = soup.find_all("div", class_="row")[7].find_all("li")


    # by sex
    source2_sex_data = {}
    for i in range(2):
        txt = data_elements[i].text
        lparen = txt.index("(")
        pct = txt.index("%")
        # male 1, female 0
        source2_sex_data[i ^ 1] = float(txt[lparen + 1 : pct])

    print(source2_sex_data)

    # by age
    # the dash is obscure, ord() of it is 8211
    source2_age_data = []
    for i in range(2, 6):
        txt = data_elements[i].text
        cur = []
        if i == 5:
            # 65+
            cur.append(65)
            cur.append(200)
        else:
            dash = txt.index(chr(8211))
            # lower upper age
            cur.append(int(txt[dash - 2 : dash]))
            cur.append(int(txt[dash + 1 : dash + 3]))
        lparen = txt.index("(")
        pct = txt.index("%")
        cur.append(float(txt[lparen + 1 : pct]))
        source2_age_data.append(cur)

    print(source2_age_data)
    print(f'Scrape output: {1} {1}')

    url = "https://wayback.archive-it.org/5774/20211119125806/https:/www.healthypeople.gov/2020/data-search/Search-the-Data?nid=5342" # site to scrape
    response = requests.get(url).text
    soup = BeautifulSoup(response, "html.parser")
    
    tab = soup.find_all("div", {"class": "ds-data-table-container"})[0]
    kid_rates = []
    info_2017 = tab.find_all("span", {"class": "ds-data-point ds-2017"})
    for index in range(0, len(info_2017)):
        span = info_2017[index].find_all("span")
        for index2 in range(0, len(span)):
            if span[index2]['class'][0] == "dp-data-estimate":
                if span[index2].text != '':
                    rate = span[index2].text
                    kid_rates.append(rate)
    
    # print(kid_rates)
    
    # smoking rates for kids aged 14, 15, 16, 17
    # age group 14-17
    kid_age_rates = kid_rates[-4:]
    
    for i in range(4):
        kid_age_rates[i] = float(kid_age_rates[i])
    avg = sum(kid_age_rates)/4
    
    new_data = [14, 17, avg]

    source2_age_data.insert(0, new_data)
    print(new_data)

    # to_exp = {'source1_data': source1_data, 'source2_sex_data': source2_sex_data, 'source2_age_data': source2_age_data}
    # # Serializing json
    # json_object = json.dumps(to_exp, indent=4)
    
    # # Writing to sample.json
    # with open("/usr/local/airflow/scraped_data.json", "w") as outfile:
    #     json_object.write(json_object)
    #  # Upload the CSV file to S3 using boto3
    # bucket_name = "de300-group11-mwaa-output"
    # key = "scraped_data.json"

    # s3_client = boto3.client('s3')
    # s3_client.upload_file("/usr/local/airflow/scraped_data.json", bucket_name, key)

    return {'source1_data': source1_data, 'source2_sex_data': source2_sex_data, 'source2_age_data': source2_age_data}

def merge_func(**kwargs):
    print('Running Data Merge')

    ti = kwargs['ti']
    scrape_data = ti.xcom_pull(task_ids='scrape')

    source1_data = scrape_data['source1_data']
    source2_sex_data = scrape_data['source2_sex_data']
    source2_age_data = scrape_data['source2_age_data']

    print(source1_data)
    print(source2_sex_data)
    print(source2_age_data)

    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme_fe)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")
    
    # Define the source1 data
    # source1_data = [[18, 24, 8.3], [25, 34, 10.6], [35, 44, 11.7], [45, 54, 13.6], [55, 64, 13.7], [65, 74, 8.9], [75, 200, 3.4]]

    # Create a new column to store the imputed values from "smoke_source1"
    df_imputed = data.withColumn(
        "smoke_source1",
        when(data["smoke"].isNull(), lit(None)).otherwise(data["smoke"]),
    )

    # Impute missing values in the "smoke" column based on age group using "smoke_source1"
    for age_range in source1_data:
        lower_age, upper_age, smoking_rate = age_range
        df_imputed = df_imputed.withColumn(
            "smoke_source1",
            when(
                (df_imputed["smoke_source1"].isNull())
                & (df_imputed["age"].between(lower_age, upper_age)),
                smoking_rate / 100,
            ).otherwise(df_imputed["smoke_source1"]),
        )

    df_imputed.show()

    # source2_age_data = [[18, 24, 5.3], [25, 44, 12.6], [45, 66, 14.9], [65, 200, 8.3]]

    # For sex, 1=male 0=female
    # source2_sex_data = {
    #     1: 13.1,
    #     0: 10.1
    # }

    # Create a new column to store the imputed values from "smoke_source2"
    df_imputed = df_imputed.withColumn(
        "smoke_source2",
        when(df_imputed["smoke"].isNull(), lit(None)).otherwise(df_imputed["smoke"]),
    )

    # Impute missing values in the "smoke" column based on age groups for female patients using "smoke_source2"
    for age_range in source2_age_data:
        lower_age, upper_age, smoking_rate = age_range
        df_imputed = df_imputed.withColumn(
            "smoke_source2",
            when(
                (df_imputed["smoke_source2"].isNull())
                & (df_imputed["sex"] == 0)
                & (df_imputed["age"].between(lower_age, upper_age)),
                smoking_rate / 100,
            ).otherwise(df_imputed["smoke_source2"]),
        )

    # Calculate the ratio of smoking rates between men and women
    smoking_rate_ratio = source2_sex_data["0"] / source2_sex_data["1"]

    # Impute missing values in the "smoke" column based on age groups for male patients using "smoke_source2"
    for age_range in source2_age_data:
        lower_age, upper_age, smoking_rate = age_range
        df_imputed = df_imputed.withColumn(
            "smoke_source2",
            when(
                (df_imputed["smoke_source2"].isNull())
                & (df_imputed["sex"] == 1)
                & (df_imputed["age"].between(lower_age, upper_age)),
                (smoking_rate * smoking_rate_ratio) / 100,
            ).otherwise(df_imputed["smoke_source2"]),
        )

    df_imputed.dtypes

    df_imputed.show()

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = df_imputed.toPandas()

    # Save the Pandas DataFrame to a CSV file
    csv_data = pandas_df.to_csv(index=False)

    # Write the CSV data to S3 using s3_hook
    hook.load_string(csv_data, key="merged_data.csv", bucket_name="de300-group11-mwaa-output", replace=True)

    return {'value1': 1, 'value2': 1}

def added_data_lr_func(**kwargs):
    print('Running Added Data LR')

    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme_fe)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")

    # Drop smoke column
    data = data.drop("smoke")

    # Convert the target column to numeric if it's not already
    if "target" not in data.columns:
        raise ValueError("The DataFrame does not contain a 'target' column.")
    target_indexer = StringIndexer(inputCol="target", outputCol="label")
    df_labeled = target_indexer.fit(data).transform(data)

    # Calculate the proportions of positive and negative labels
    label_counts = df_labeled.groupBy("label").count().collect()
    positive_label_count = label_counts[1][1]
    total_count = df_labeled.count()

    # Calculate the proportions for stratification
    positive_proportion = positive_label_count / total_count
    negative_proportion = 1 - positive_proportion

    # Calculate the desired number of samples for each class
    train_positive_count = int(0.9 * positive_label_count)
    train_negative_count = int(0.9 * (total_count - positive_label_count))

    # Perform the stratified split with the desired counts
    train_data = df_labeled.sampleBy(
        "label",
        fractions={
            0: train_negative_count / (total_count - positive_label_count),
            1: train_positive_count / positive_label_count,
        },
        seed=42,
    )
    test_data = df_labeled.subtract(train_data)

    # Verify the proportions of positive labels in both sets
    train_positive_count = train_data.filter(col("label") == 1).count()
    test_positive_count = test_data.filter(col("label") == 1).count()

    train_positive_proportion = train_positive_count / train_data.count()
    test_positive_proportion = test_positive_count / test_data.count()

    print(
        f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
    )
    print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

    train_data.count()

    test_data.count()

    # Assuming you have already split the data into train_data and test_data

    # Define the feature columns
    feature_columns = [
        "age",
        "sex",
        "painloc",
        "painexer",
        "cp",
        "trestbps",
        "fbs",
        "prop",
        "nitr",
        "pro",
        "diuretic",
        "thaldur",
        "thalach",
        "exang",
        "oldpeak",
        "slope",
        "trestbpsxthalach",
    ]

    # Define the target column
    target_column = "target"

    # Convert string columns to appropriate data types
    for column in feature_columns + [target_column]:
        train_data = train_data.withColumn(column, train_data[column].cast("double"))

    # Convert the test data columns to appropriate data types
    for column in feature_columns + [target_column]:
        test_data = test_data.withColumn(column, test_data[column].cast("double"))

    # Create the vector assembler
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

    # Apply MinMaxScaling transformation
    scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

    # Transform the training data
    train_data = assembler.transform(train_data)

    train_data = scaler.fit(train_data).transform(train_data)

    # Define the classification models
    logistic_regression = LogisticRegressionSpark(featuresCol="features", labelCol=target_column)
    # logistic_regression = LogisticRegressionSp(labelCol=target_column)

    # Define the parameter grids for hyperparameter tuning
    param_grid_lr = (
        ParamGridBuilder().addGrid(logistic_regression.regParam, [0.01, 0.1]).build()
    )

    # Create the pipeline for each model
    pipeline_lr = PipelineSpark(stages=[logistic_regression])
    
    # Create the evaluator for binary classification
    evaluator = BinaryClassificationEvaluator(labelCol=target_column)

    # Perform cross-validation and hyperparameter tuning for each model
    cv_lr = CrossValidator(
        estimator=pipeline_lr,
        estimatorParamMaps=param_grid_lr,
        evaluator=evaluator,
        numFolds=5,
    )

    # Fit the models to the training data
    cv_model_lr = cv_lr.fit(train_data)

    # Transform the test data
    test_data = assembler.transform(test_data)

    test_data = scaler.fit(test_data).transform(test_data)

    # Evaluate the models on the test data
    result_lr = cv_model_lr.transform(test_data)

    # Evaluate performance metrics
    auc_lr = evaluator.evaluate(result_lr)

    print(auc_lr)

    # Get the best model for logistic regression
    best_model_lr = cv_model_lr.bestModel
    # Extract the best hyperparameters
    best_params_lr = best_model_lr.stages[0].extractParamMap()
    best_reg_param = best_params_lr[logistic_regression.regParam]
    # Print the best hyperparameters
    print("Best Hyperparameters for Logistic Regression:")
    print(f"Regularization Parameter: {best_reg_param}")

    return {'rocauc': auc_lr}

def added_data_svm_func(**kwargs):
    print('Running Added Data SVM')

    spark = SparkSession.builder.getOrCreate()

    s3_key = 'sprk_fe_data.csv'
    s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

    # Create a Pandas DataFrame
    pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

    # Convert all columns to double type
    for column in pandas_df.columns:
        pandas_df[column] = pandas_df[column].astype(float)

    # Create a Spark DataFrame by explicitly specifying the schema
    df = spark.createDataFrame(pandas_df, limited_scheme_fe)

    # Create a temporary view from the Spark DataFrame
    df.createOrReplaceTempView("temp_view")

    # Query the temporary view using Spark SQL
    data = spark.sql("SELECT * FROM temp_view")

    # Drop smoke column
    data = data.drop("smoke")

    # Convert the target column to numeric if it's not already
    if "target" not in data.columns:
        raise ValueError("The DataFrame does not contain a 'target' column.")
    target_indexer = StringIndexer(inputCol="target", outputCol="label")
    df_labeled = target_indexer.fit(data).transform(data)

    # Calculate the proportions of positive and negative labels
    label_counts = df_labeled.groupBy("label").count().collect()
    positive_label_count = label_counts[1][1]
    total_count = df_labeled.count()

    # Calculate the proportions for stratification
    positive_proportion = positive_label_count / total_count
    negative_proportion = 1 - positive_proportion

    # Calculate the desired number of samples for each class
    train_positive_count = int(0.9 * positive_label_count)
    train_negative_count = int(0.9 * (total_count - positive_label_count))

    # Perform the stratified split with the desired counts
    train_data_svm = df_labeled.sampleBy(
        "label",
        fractions={
            0: train_negative_count / (total_count - positive_label_count),
            1: train_positive_count / positive_label_count,
        },
        seed=42,
    )
    test_data_svm = df_labeled.subtract(train_data_svm)

    # Verify the proportions of positive labels in both sets
    train_positive_count = train_data_svm.filter(col("label") == 1).count()
    test_positive_count = test_data_svm.filter(col("label") == 1).count()

    train_positive_proportion = train_positive_count / train_data_svm.count()
    test_positive_proportion = test_positive_count / test_data_svm.count()

    print(
        f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
    )
    print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

    train_data_svm.count()

    test_data_svm.count()

    # Assuming you have already split the data into train_data_svm and test_data_svm

    # Define the feature columns
    feature_columns = [
        "age",
        "sex",
        "painloc",
        "painexer",
        "cp",
        "trestbps",
        "fbs",
        "prop",
        "nitr",
        "pro",
        "diuretic",
        "thaldur",
        "thalach",
        "exang",
        "oldpeak",
        "slope",
        "trestbpsxthalach",
    ]

    # Define the target column
    target_column = "target"

    # Convert string columns to appropriate data types
    for column in feature_columns + [target_column]:
        train_data_svm = train_data_svm.withColumn(column, train_data_svm[column].cast("double"))

    # Convert the test data columns to appropriate data types
    for column in feature_columns + [target_column]:
        test_data_svm = test_data_svm.withColumn(column, test_data_svm[column].cast("double"))

    # Create the vector assembler
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

    # Apply MinMaxScaling transformation
    scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

    # Transform the training data
    train_data_svm = assembler.transform(train_data_svm)

    train_data_svm = scaler.fit(train_data_svm).transform(train_data_svm)

    # Define the classification models
    svm = LinearSVC(featuresCol="features", labelCol=target_column)
    # logistic_regression = LogisticRegressionSp(labelCol=target_column)

    # Define the parameter grids for hyperparameter tuning
    # Define the parameter grid for hyperparameter tuning
    param_grid_svm = (
        ParamGridBuilder()
        .addGrid(svm.regParam, [0.01, 0.1])
        .addGrid(svm.maxIter, [10, 100])
        .build()
    )

    # Create the pipeline for the SVM model
    pipeline_svm = PipelineSpark(stages=[svm])
    
    # Create the evaluator for binary classification
    evaluator = BinaryClassificationEvaluator(labelCol=target_column)

    # Perform cross-validation and hyperparameter tuning for the SVM model
    cv_svm = CrossValidator(
        estimator=pipeline_svm,
        estimatorParamMaps=param_grid_svm,
        evaluator=evaluator,
        numFolds=5,
    )

    # Fit the SVM model to the training data
    cv_model_svm = cv_svm.fit(train_data_svm)

    # Transform the test data for the SVM model
    test_data_svm = assembler.transform(test_data_svm)
    test_data_svm = scaler.fit(test_data_svm).transform(test_data_svm)

    # Evaluate the SVM model on the test data
    result_svm = cv_model_svm.transform(test_data_svm)

    # Evaluate performance metrics for the SVM model
    auc_svm = evaluator.evaluate(result_svm)
    print(auc_svm)

    # Get the best model for the SVM model
    best_model_svm = cv_model_svm.bestModel
    # Extract the best hyperparameters for the SVM model
    best_params_svm = best_model_svm.stages[0].extractParamMap()
    best_reg_param_svm = best_params_svm[svm.regParam]
    best_max_iter_svm = best_params_svm[svm.maxIter]

    # Print the best hyperparameters for the SVM model
    print("Best Hyperparameters for Added Data SVM:")
    print(f"Regularization Parameter: {best_reg_param_svm}")
    print(f"Maximum Iterations: {best_max_iter_svm}")
    return {'rocauc': auc_svm}


def best_model_func(**kwargs):
    print('Running Best Model')
    
    ti = kwargs['ti']
    sprk_svm_return_value = ti.xcom_pull(task_ids='sprk_svm')
    sprk_lr_return_value = ti.xcom_pull(task_ids='sprk_lr')
    sklrn_svm_return_value = ti.xcom_pull(task_ids='sklrn_svm')
    sklrn_lr_return_value = ti.xcom_pull(task_ids='sklrn_lr')
    added_data_svm_return_value = ti.xcom_pull(task_ids='added_data_svm')
    added_data_lr_return_value = ti.xcom_pull(task_ids='added_data_lr')

    dictionaries = [
        sprk_lr_return_value,
        sprk_svm_return_value,
        sklrn_lr_return_value,
        sklrn_svm_return_value,
        added_data_lr_return_value,
        added_data_svm_return_value,
    ]

    # Initialize variables to track the maximum value and corresponding variable name
    max_rocauc = float('-inf')
    best_variable = ''

    # Iterate over the dictionaries
    for i, dictionary in enumerate(dictionaries):
        if 'rocauc' in dictionary:
            rocauc = dictionary['rocauc']
            if rocauc > max_rocauc:
                max_rocauc = rocauc
                best_variable = i

    # Check if a best variable was found
    if best_variable:
        print(f"The best 'rocauc' value is {max_rocauc}, found in {best_variable}.")
    else:
        print("No 'rocauc' values found.")
    
    return {'best_model': best_variable}

def evaluate_func(**kwargs):
    print('Running Evaluate')
    ti = kwargs['ti']
    best_model_ret_val = ti.xcom_pull(task_ids='best_model')
    print("Best Model returned: ", best_model_ret_val['best_model'])

    roc_auc = 0

    if best_model_ret_val["best_model"] == 0:
        spark = SparkSession.builder.getOrCreate()

        s3_key = 'sprk_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Convert all columns to double type
        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(float)

        # Create a Spark DataFrame by explicitly specifying the schema
        df = spark.createDataFrame(pandas_df, limited_scheme_fe)

        # Create a temporary view from the Spark DataFrame
        df.createOrReplaceTempView("temp_view")

        # Query the temporary view using Spark SQL
        data = spark.sql("SELECT * FROM temp_view")

        # Drop smoke column
        data = data.drop("smoke")

        # Convert the target column to numeric if it's not already
        if "target" not in data.columns:
            raise ValueError("The DataFrame does not contain a 'target' column.")
        target_indexer = StringIndexer(inputCol="target", outputCol="label")
        df_labeled = target_indexer.fit(data).transform(data)

        # Calculate the proportions of positive and negative labels
        label_counts = df_labeled.groupBy("label").count().collect()
        positive_label_count = label_counts[1][1]
        total_count = df_labeled.count()

        # Calculate the proportions for stratification
        positive_proportion = positive_label_count / total_count
        negative_proportion = 1 - positive_proportion

        # Calculate the desired number of samples for each class
        train_positive_count = int(0.9 * positive_label_count)
        train_negative_count = int(0.9 * (total_count - positive_label_count))

        # Perform the stratified split with the desired counts
        train_data = df_labeled.sampleBy(
            "label",
            fractions={
                0: train_negative_count / (total_count - positive_label_count),
                1: train_positive_count / positive_label_count,
            },
            seed=42,
        )
        test_data = df_labeled.subtract(train_data)

        # Verify the proportions of positive labels in both sets
        train_positive_count = train_data.filter(col("label") == 1).count()
        test_positive_count = test_data.filter(col("label") == 1).count()

        train_positive_proportion = train_positive_count / train_data.count()
        test_positive_proportion = test_positive_count / test_data.count()

        print(
            f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
        )
        print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

        train_data.count()

        test_data.count()

        # Assuming you have already split the data into train_data and test_data

        # Define the feature columns
        feature_columns = [
            "age",
            "sex",
            "painloc",
            "painexer",
            "cp",
            "trestbps",
            "fbs",
            "prop",
            "nitr",
            "pro",
            "diuretic",
            "thaldur",
            "thalach",
            "exang",
            "oldpeak",
            "slope",
            "trestbpsxthalach",
        ]

        # Define the target column
        target_column = "target"

        # Convert string columns to appropriate data types
        for column in feature_columns + [target_column]:
            train_data = train_data.withColumn(column, train_data[column].cast("double"))

        # Convert the test data columns to appropriate data types
        for column in feature_columns + [target_column]:
            test_data = test_data.withColumn(column, test_data[column].cast("double"))

        # Create the vector assembler
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

        # Apply MinMaxScaling transformation
        scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

        # Transform the training data
        train_data = assembler.transform(train_data)

        train_data = scaler.fit(train_data).transform(train_data)

        # Define the classification models
        logistic_regression = LogisticRegressionSpark(featuresCol="features", labelCol=target_column)
        # logistic_regression = LogisticRegressionSp(labelCol=target_column)

        # Define the parameter grids for hyperparameter tuning
        param_grid_lr = (
            ParamGridBuilder().addGrid(logistic_regression.regParam, [0.01, 0.1]).build()
        )

        # Create the pipeline for each model
        pipeline_lr = PipelineSpark(stages=[logistic_regression])
        
        # Create the evaluator for binary classification
        evaluator = BinaryClassificationEvaluator(labelCol=target_column)

        # Perform cross-validation and hyperparameter tuning for each model
        cv_lr = CrossValidator(
            estimator=pipeline_lr,
            estimatorParamMaps=param_grid_lr,
            evaluator=evaluator,
            numFolds=5,
        )

        # Fit the models to the training data
        cv_model_lr = cv_lr.fit(train_data)

        # Transform the test data
        test_data = assembler.transform(test_data)

        test_data = scaler.fit(test_data).transform(test_data)

        # Evaluate the models on the test data
        result_lr = cv_model_lr.transform(test_data)

        # Evaluate performance metrics
        auc_lr = evaluator.evaluate(result_lr)

        roc_auc = auc_lr

        print(auc_lr)

        # Get the best model for logistic regression
        best_model_lr = cv_model_lr.bestModel
        # Extract the best hyperparameters
        best_params_lr = best_model_lr.stages[0].extractParamMap()
        best_reg_param = best_params_lr[logistic_regression.regParam]
        # Print the best hyperparameters
        print("Best Hyperparameters for Logistic Regression:")
        print(f"Regularization Parameter: {best_reg_param}")

    elif best_model_ret_val["best_model"] == 1:

        spark = SparkSession.builder.getOrCreate()

        s3_key = 'sprk_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Convert all columns to double type
        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(float)

        # Create a Spark DataFrame by explicitly specifying the schema
        df = spark.createDataFrame(pandas_df, limited_scheme_fe)

        # Create a temporary view from the Spark DataFrame
        df.createOrReplaceTempView("temp_view")

        # Query the temporary view using Spark SQL
        data = spark.sql("SELECT * FROM temp_view")

        # Drop smoke column
        data = data.drop("smoke")

        # Convert the target column to numeric if it's not already
        if "target" not in data.columns:
            raise ValueError("The DataFrame does not contain a 'target' column.")
        target_indexer = StringIndexer(inputCol="target", outputCol="label")
        df_labeled = target_indexer.fit(data).transform(data)

        # Calculate the proportions of positive and negative labels
        label_counts = df_labeled.groupBy("label").count().collect()
        positive_label_count = label_counts[1][1]
        total_count = df_labeled.count()

        # Calculate the proportions for stratification
        positive_proportion = positive_label_count / total_count
        negative_proportion = 1 - positive_proportion

        # Calculate the desired number of samples for each class
        train_positive_count = int(0.9 * positive_label_count)
        train_negative_count = int(0.9 * (total_count - positive_label_count))

        # Perform the stratified split with the desired counts
        train_data_svm = df_labeled.sampleBy(
            "label",
            fractions={
                0: train_negative_count / (total_count - positive_label_count),
                1: train_positive_count / positive_label_count,
            },
            seed=42,
        )
        test_data_svm = df_labeled.subtract(train_data_svm)

        # Verify the proportions of positive labels in both sets
        train_positive_count = train_data_svm.filter(col("label") == 1).count()
        test_positive_count = test_data_svm.filter(col("label") == 1).count()

        train_positive_proportion = train_positive_count / train_data_svm.count()
        test_positive_proportion = test_positive_count / test_data_svm.count()

        print(
            f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
        )
        print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

        train_data_svm.count()

        test_data_svm.count()

        # Assuming you have already split the data into train_data_svm and test_data_svm

        # Define the feature columns
        feature_columns = [
            "age",
            "sex",
            "painloc",
            "painexer",
            "cp",
            "trestbps",
            "fbs",
            "prop",
            "nitr",
            "pro",
            "diuretic",
            "thaldur",
            "thalach",
            "exang",
            "oldpeak",
            "slope",
            "trestbpsxthalach",
        ]

        # Define the target column
        target_column = "target"

        # Convert string columns to appropriate data types
        for column in feature_columns + [target_column]:
            train_data_svm = train_data_svm.withColumn(column, train_data_svm[column].cast("double"))

        # Convert the test data columns to appropriate data types
        for column in feature_columns + [target_column]:
            test_data_svm = test_data_svm.withColumn(column, test_data_svm[column].cast("double"))

        # Create the vector assembler
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

        # Apply MinMaxScaling transformation
        scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

        # Transform the training data
        train_data_svm = assembler.transform(train_data_svm)

        train_data_svm = scaler.fit(train_data_svm).transform(train_data_svm)

        # Define the classification models
        svm = LinearSVC(featuresCol="features", labelCol=target_column)
        # logistic_regression = LogisticRegressionSp(labelCol=target_column)

        # Define the parameter grids for hyperparameter tuning
        # Define the parameter grid for hyperparameter tuning
        param_grid_svm = (
            ParamGridBuilder()
            .addGrid(svm.regParam, [0.01, 0.1])
            .addGrid(svm.maxIter, [10, 100])
            .build()
        )

        # Create the pipeline for the SVM model
        pipeline_svm = PipelineSpark(stages=[svm])
        
        # Create the evaluator for binary classification
        evaluator = BinaryClassificationEvaluator(labelCol=target_column)

        # Perform cross-validation and hyperparameter tuning for the SVM model
        cv_svm = CrossValidator(
            estimator=pipeline_svm,
            estimatorParamMaps=param_grid_svm,
            evaluator=evaluator,
            numFolds=5,
        )

        # Fit the SVM model to the training data
        cv_model_svm = cv_svm.fit(train_data_svm)

        # Transform the test data for the SVM model
        test_data_svm = assembler.transform(test_data_svm)
        test_data_svm = scaler.fit(test_data_svm).transform(test_data_svm)

        # Evaluate the SVM model on the test data
        result_svm = cv_model_svm.transform(test_data_svm)

        # Evaluate performance metrics for the SVM model
        auc_svm = evaluator.evaluate(result_svm)
        print(auc_svm)

        roc_auc = auc_svm

        # Get the best model for the SVM model
        best_model_svm = cv_model_svm.bestModel
        # Extract the best hyperparameters for the SVM model
        best_params_svm = best_model_svm.stages[0].extractParamMap()
        best_reg_param_svm = best_params_svm[svm.regParam]
        best_max_iter_svm = best_params_svm[svm.maxIter]

        # Print the best hyperparameters for the SVM model
        print("Best Hyperparameters for SVM:")
        print(f"Regularization Parameter: {best_reg_param_svm}")
        print(f"Maximum Iterations: {best_max_iter_svm}")

    elif best_model_ret_val["best_model"] == 2:

        s3_key = 'sklearn_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        model_data = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Get a list of all column names
        column_names = list(model_data.columns)

        # Remove the "target" column if present
        if "target" in column_names:
            column_names.remove("target")


        # Step 3: Prepare your data
        X = model_data[column_names]  # Features (independent variables)
        y = model_data['target']  # Target variable (dependent variable)

        # Step 4: Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)

        # Step 5: Create an instance of LogisticRegression
        model = LogisticRegression()

        # Step 6: Fit the logistic regression model to the training data
        model.fit(X_train, y_train)

        y_prob = model.predict_proba(X_test)[:, 1]  # Predicted probabilities for class 1
        y_pred = model.predict(X_test)

        # Step 8: Evaluate the performance of the model
        accuracy = accuracy_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_prob)
        print('Accuracy:', accuracy)
        print('ROC AUC:', roc_auc)

    elif best_model_ret_val["best_model"] == 2:

        s3_key = 'sklearn_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        model_data = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Get a list of all column names
        column_names = list(model_data.columns)

        # Remove the "target" column if present
        if "target" in column_names:
            column_names.remove("target")

        # Step 3: Prepare your data
        X = model_data[column_names]  # Features (independent variables)
        y = model_data['target']  # Target variable (dependent variable)

        # Step 4: Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, stratify=y, random_state=42)

        # Step 5: Create an instance of LinearSVC
        model = LinearSVCSklearn(C=1.0)

        # Step 6: Fit the LinearSVC model to the training data
        model.fit(X_train, y_train)

        # Step 7: Predict the target variable for the testing data
        y_pred = model.predict(X_test)

        # Step 8: Evaluate the performance of the model
        accuracy = accuracy_score(y_test, y_pred)

        # Step 7: Obtain the decision function scores for the testing data
        decision_scores = model.decision_function(X_test)

        # Step 8: Convert the target variable to binary form
        y_test_binary = label_binarize(y_test, classes=model.classes_)

        # Step 9: Calculate the ROC AUC score
        roc_auc = roc_auc_score(y_test_binary, decision_scores)
        print('ROC AUC:', roc_auc)
        print('Accuracy:', accuracy)
    
    elif best_model_ret_val["best_model"] == 4:

        spark = SparkSession.builder.getOrCreate()

        s3_key = 'sprk_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Convert all columns to double type
        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(float)

        # Create a Spark DataFrame by explicitly specifying the schema
        df = spark.createDataFrame(pandas_df, limited_scheme_fe)

        # Create a temporary view from the Spark DataFrame
        df.createOrReplaceTempView("temp_view")

        # Query the temporary view using Spark SQL
        data = spark.sql("SELECT * FROM temp_view")

        # Drop smoke column
        data = data.drop("smoke")

        # Convert the target column to numeric if it's not already
        if "target" not in data.columns:
            raise ValueError("The DataFrame does not contain a 'target' column.")
        target_indexer = StringIndexer(inputCol="target", outputCol="label")
        df_labeled = target_indexer.fit(data).transform(data)

        # Calculate the proportions of positive and negative labels
        label_counts = df_labeled.groupBy("label").count().collect()
        positive_label_count = label_counts[1][1]
        total_count = df_labeled.count()

        # Calculate the proportions for stratification
        positive_proportion = positive_label_count / total_count
        negative_proportion = 1 - positive_proportion

        # Calculate the desired number of samples for each class
        train_positive_count = int(0.9 * positive_label_count)
        train_negative_count = int(0.9 * (total_count - positive_label_count))

        # Perform the stratified split with the desired counts
        train_data = df_labeled.sampleBy(
            "label",
            fractions={
                0: train_negative_count / (total_count - positive_label_count),
                1: train_positive_count / positive_label_count,
            },
            seed=42,
        )
        test_data = df_labeled.subtract(train_data)

        # Verify the proportions of positive labels in both sets
        train_positive_count = train_data.filter(col("label") == 1).count()
        test_positive_count = test_data.filter(col("label") == 1).count()

        train_positive_proportion = train_positive_count / train_data.count()
        test_positive_proportion = test_positive_count / test_data.count()

        print(
            f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
        )
        print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

        train_data.count()

        test_data.count()

        # Assuming you have already split the data into train_data and test_data

        # Define the feature columns
        feature_columns = [
            "age",
            "sex",
            "painloc",
            "painexer",
            "cp",
            "trestbps",
            "fbs",
            "prop",
            "nitr",
            "pro",
            "diuretic",
            "thaldur",
            "thalach",
            "exang",
            "oldpeak",
            "slope",
            "trestbpsxthalach",
        ]

        # Define the target column
        target_column = "target"

        # Convert string columns to appropriate data types
        for column in feature_columns + [target_column]:
            train_data = train_data.withColumn(column, train_data[column].cast("double"))

        # Convert the test data columns to appropriate data types
        for column in feature_columns + [target_column]:
            test_data = test_data.withColumn(column, test_data[column].cast("double"))

        # Create the vector assembler
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

        # Apply MinMaxScaling transformation
        scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

        # Transform the training data
        train_data = assembler.transform(train_data)

        train_data = scaler.fit(train_data).transform(train_data)

        # Define the classification models
        logistic_regression = LogisticRegressionSpark(featuresCol="features", labelCol=target_column)
        # logistic_regression = LogisticRegressionSp(labelCol=target_column)

        # Define the parameter grids for hyperparameter tuning
        param_grid_lr = (
            ParamGridBuilder().addGrid(logistic_regression.regParam, [0.01, 0.1]).build()
        )

        # Create the pipeline for each model
        pipeline_lr = PipelineSpark(stages=[logistic_regression])
        
        # Create the evaluator for binary classification
        evaluator = BinaryClassificationEvaluator(labelCol=target_column)

        # Perform cross-validation and hyperparameter tuning for each model
        cv_lr = CrossValidator(
            estimator=pipeline_lr,
            estimatorParamMaps=param_grid_lr,
            evaluator=evaluator,
            numFolds=5,
        )

        # Fit the models to the training data
        cv_model_lr = cv_lr.fit(train_data)

        # Transform the test data
        test_data = assembler.transform(test_data)

        test_data = scaler.fit(test_data).transform(test_data)

        # Evaluate the models on the test data
        result_lr = cv_model_lr.transform(test_data)

        # Evaluate performance metrics
        auc_lr = evaluator.evaluate(result_lr)

        roc_auc = auc_lr

        print(auc_lr)

        # Get the best model for logistic regression
        best_model_lr = cv_model_lr.bestModel
        # Extract the best hyperparameters
        best_params_lr = best_model_lr.stages[0].extractParamMap()
        best_reg_param = best_params_lr[logistic_regression.regParam]
        # Print the best hyperparameters
        print("Best Hyperparameters for Logistic Regression:")
        print(f"Regularization Parameter: {best_reg_param}")


    elif best_model_ret_val["best_model"] == 5:

        spark = SparkSession.builder.getOrCreate()

        s3_key = 'sprk_fe_data.csv'
        s3_data = hook.read_key(key=s3_key, bucket_name="de300-group11-mwaa-output")

        # Create a Pandas DataFrame
        pandas_df = pd.read_csv(StringIO(s3_data), encoding='utf-8')

        # Convert all columns to double type
        for column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(float)

        # Create a Spark DataFrame by explicitly specifying the schema
        df = spark.createDataFrame(pandas_df, limited_scheme_fe)

        # Create a temporary view from the Spark DataFrame
        df.createOrReplaceTempView("temp_view")

        # Query the temporary view using Spark SQL
        data = spark.sql("SELECT * FROM temp_view")

        # Drop smoke column
        data = data.drop("smoke")

        # Convert the target column to numeric if it's not already
        if "target" not in data.columns:
            raise ValueError("The DataFrame does not contain a 'target' column.")
        target_indexer = StringIndexer(inputCol="target", outputCol="label")
        df_labeled = target_indexer.fit(data).transform(data)

        # Calculate the proportions of positive and negative labels
        label_counts = df_labeled.groupBy("label").count().collect()
        positive_label_count = label_counts[1][1]
        total_count = df_labeled.count()

        # Calculate the proportions for stratification
        positive_proportion = positive_label_count / total_count
        negative_proportion = 1 - positive_proportion

        # Calculate the desired number of samples for each class
        train_positive_count = int(0.9 * positive_label_count)
        train_negative_count = int(0.9 * (total_count - positive_label_count))

        # Perform the stratified split with the desired counts
        train_data_svm = df_labeled.sampleBy(
            "label",
            fractions={
                0: train_negative_count / (total_count - positive_label_count),
                1: train_positive_count / positive_label_count,
            },
            seed=42,
        )
        test_data_svm = df_labeled.subtract(train_data_svm)

        # Verify the proportions of positive labels in both sets
        train_positive_count = train_data_svm.filter(col("label") == 1).count()
        test_positive_count = test_data_svm.filter(col("label") == 1).count()

        train_positive_proportion = train_positive_count / train_data_svm.count()
        test_positive_proportion = test_positive_count / test_data_svm.count()

        print(
            f"Proportion of positive labels in the training set: {train_positive_proportion:.2f}"
        )
        print(f"Proportion of positive labels in the test set: {test_positive_proportion:.2f}")

        train_data_svm.count()

        test_data_svm.count()

        # Assuming you have already split the data into train_data_svm and test_data_svm

        # Define the feature columns
        feature_columns = [
            "age",
            "sex",
            "painloc",
            "painexer",
            "cp",
            "trestbps",
            "fbs",
            "prop",
            "nitr",
            "pro",
            "diuretic",
            "thaldur",
            "thalach",
            "exang",
            "oldpeak",
            "slope",
            "trestbpsxthalach",
        ]

        # Define the target column
        target_column = "target"

        # Convert string columns to appropriate data types
        for column in feature_columns + [target_column]:
            train_data_svm = train_data_svm.withColumn(column, train_data_svm[column].cast("double"))

        # Convert the test data columns to appropriate data types
        for column in feature_columns + [target_column]:
            test_data_svm = test_data_svm.withColumn(column, test_data_svm[column].cast("double"))

        # Create the vector assembler
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features2scale")

        # Apply MinMaxScaling transformation
        scaler = MinMaxScalerSpark(inputCol="features2scale", outputCol="features")

        # Transform the training data
        train_data_svm = assembler.transform(train_data_svm)

        train_data_svm = scaler.fit(train_data_svm).transform(train_data_svm)

        # Define the classification models
        svm = LinearSVC(featuresCol="features", labelCol=target_column)
        # logistic_regression = LogisticRegressionSp(labelCol=target_column)

        # Define the parameter grids for hyperparameter tuning
        # Define the parameter grid for hyperparameter tuning
        param_grid_svm = (
            ParamGridBuilder()
            .addGrid(svm.regParam, [0.01, 0.1])
            .addGrid(svm.maxIter, [10, 100])
            .build()
        )

        # Create the pipeline for the SVM model
        pipeline_svm = PipelineSpark(stages=[svm])
        
        # Create the evaluator for binary classification
        evaluator = BinaryClassificationEvaluator(labelCol=target_column)

        # Perform cross-validation and hyperparameter tuning for the SVM model
        cv_svm = CrossValidator(
            estimator=pipeline_svm,
            estimatorParamMaps=param_grid_svm,
            evaluator=evaluator,
            numFolds=5,
        )

        # Fit the SVM model to the training data
        cv_model_svm = cv_svm.fit(train_data_svm)

        # Transform the test data for the SVM model
        test_data_svm = assembler.transform(test_data_svm)
        test_data_svm = scaler.fit(test_data_svm).transform(test_data_svm)

        # Evaluate the SVM model on the test data
        result_svm = cv_model_svm.transform(test_data_svm)

        # Evaluate performance metrics for the SVM model
        auc_svm = evaluator.evaluate(result_svm)
        print(auc_svm)

        roc_auc = auc_svm

        # Get the best model for the SVM model
        best_model_svm = cv_model_svm.bestModel
        # Extract the best hyperparameters for the SVM model
        best_params_svm = best_model_svm.stages[0].extractParamMap()
        best_reg_param_svm = best_params_svm[svm.regParam]
        best_max_iter_svm = best_params_svm[svm.maxIter]

        # Print the best hyperparameters for the SVM model
        print("Best Hyperparameters for SVM:")
        print(f"Regularization Parameter: {best_reg_param_svm}")
        print(f"Maximum Iterations: {best_max_iter_svm}")

    return {'best_roc_auc_evaluated': roc_auc}

# Instantiate the DAG
dag = DAG(
    'deproject_dag',
    default_args=default_args,
    description='A DAG for the de300 project with dependencies',
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL, # it can be @daily, ...\
    tags=["de300"]
)

# # Define the tasks
# task1 = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data_func,
#     provide_context=True,
#     dag=dag,
# )

task2 = PythonOperator(
    task_id='sklrn_cln_imp',
    python_callable=sklrn_cln_impute_func,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='sprk_cln_imp',
    python_callable=sprk_cln_impute_func,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='sklrn_fe',
    python_callable=sklrn_fe_func,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id='sprk_fe',
    python_callable=sprk_fe_func,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id='sprk_svm',
    python_callable=sprk_svm_func,
    provide_context=True,
    dag=dag,
)

task7 = PythonOperator(
    task_id='sprk_lr',
    python_callable=sprk_lr_func,
    provide_context=True,
    dag=dag,
)

task8 = PythonOperator(
    task_id='sklrn_svm',
    python_callable=sklrn_svm_func,
    provide_context=True,
    dag=dag,
)

task9 = PythonOperator(
    task_id='sklrn_lr',
    python_callable=sklrn_lr_func,
    provide_context=True,
    dag=dag,
)

task10 = PythonOperator(
    task_id='scrape',
    python_callable=scrape_func,
    provide_context=True,
    dag=dag,
)

task11 = PythonOperator(
    task_id='merge',
    python_callable=merge_func,
    provide_context=True,
    dag=dag,
)

task12 = PythonOperator(
    task_id='added_data_svm',
    python_callable=added_data_svm_func,
    provide_context=True,
    dag=dag,
)

task13 = PythonOperator(
    task_id='added_data_lr',
    python_callable=added_data_lr_func,
    provide_context=True,
    dag=dag,
)

task14 = PythonOperator(
    task_id='best_model',
    python_callable=best_model_func,
    provide_context=True,
    dag=dag,
)

task15 = PythonOperator(
    task_id='evaluate',
    python_callable=evaluate_func,
    provide_context=True,
    dag=dag,
)

# Define the dependencies
# task1 >> [task2, task3]
task2 >> task4 >> [task8, task9, task11]
task3 >> task5 >> [task6, task7, task11]
task10 >> task11 >> [task12, task13]
task6 >> task14
task7 >> task14
task8 >> task14
task9 >> task14
task12 >> task14
task13 >> task14
task14 >> task15

