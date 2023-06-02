# DataEng300 - Module 3 Workflow Orchestration

## Requirements

- AWS S3
- AWS MWAA

## Setting up AWS
### S3
Create 2 buckets for the entire workflow. In this case, we have `de300-group11-mwaa ` as the S3 bucket where the workflow for the DAG is defined and data is available at `de300-group1`. Output csv and png files are stored at `de300-group11-mwaa-output`.
### MWAA (todo)


## Tasks
Building on top of previous modules, this step required scraping another data source https://wayback.archive-it.org/5774/20211119125806/https:/www.healthypeople.gov/2020/data-search/Search-the-Data?nid=5342 in addition to the previously scraped https://www.abs.gov.au/statistics/health/health-conditions-and-risks/smoking/2020-21 and https://www.cdc.gov/tobacco/data_statistics/fact_sheets/adult_data/cig_smoking/index.htm. We looked through all the data that was available and the data that was most applicable to this study were the data about the gender and age. From this new data source we were able to create another category of age group by taking the rate of smoking for adolecents in grades 9-12 (proxying ages 14-17) and considering the data.

## AIRFLOW & DAG
We mimicked our DAG based off of the example graph in the instructions. The only difference with ours is we do not have a load data task. This is because we are pulling and pushing various csv files in every single task so the load data task is unnecessary.

A note on running the DAG: On occasion the logistic regression and/or svm tasks will fail due to spark internal errors. Most of the time they will work but sometimes they won't. 

## Model Selection
After observing all models the best rocauc score achieved was 0.8756 by the merged data logistic regression.


### Merging Step
A note on the merging step. We attempted to use the same code that worked for us in part 2 of this project to create smoke_source1 and smoke_source2 columns imputed based on the smoke column and the scraped smoking data. We do not know why the same code produces different results when running in airflow. We kept the merge task in but adjusted for this issue in the later lr and svm on the merged data. If we are able to solve this issue in the next itterration of the project we will be able to properly perform logistic regression and svm on the properly merged data.

### Note on data cleaning
We noticed that there was a lot of data in the originial heart_disease.csv file that made no sense. After further analysis we decided to simply remove all lines of the csv file after line 899 as it was purely invalid data. Finally, when we were dealing with postgres tables locally, we ran into issues with the column names ekgday(day and target, so we changed the csv file headers to be ekgday and target_column respectively.

### Missing Values and Imputation
To start we dropped all columns from the data with larger than 12% missing values. We chose 12% by observing the missing value percentages of all columns and found 12% to be a reasonable number. We were then able to visually pick out which columns were numerical and which were categorical. For columns we were unsure about, we counted the number of unique values and were then able to discern further. For imputation, we imputed numerical columns with the median and categorical columns with the mode. 

### Outliers
To identify outliers in the numerical columns we used a limit of 1.5 IQR. We found this to be a reasonable upper and lower limit and removed all rows with values outside of that range. With categorical features, it was a little more difficult to identify and remove outliers. To do so, we analyzed the value counts of the features and removed rows with specific values, such as the rows in prop equal to 22 because the rest of the values are clearly zero or one.

### Statistical Measures
The statistical measures we observed from the cleaned data can be found in statistical_measures.csv. It is important to note that the standard deviation of each column is heavily affected by skew which is fixed in the next steps we take. It is also interesting to note that there is not a single column with a value greater than 1 or a value less than 0.

### Box and Scatter Plots
Transformed box plots and scatter plots can be observed in transformed_boxplots.png and all_scatter_plots.png. When observing the scatter plots in particular it is interesting to note the various correlations between columns. For example there is a clear positive correlation between dummy and trestbps and a slight positive correlation between cday and ekgday. That said there are also many colums that show no correlation between themselves. We will explore these relationships as we progress through the remainder of the project.