# Student Data Analysis
---
## Case Study
To undestand the students performance, the customer experience of a paid school asked the data team some data to analyse how the students have been performing since last quarter. To accomplish that, some business questions were formed so that one could measure students performance.

The goal here is to discover the students who needs help, design a solution so that we could avoid churn and calculate the amount of money that keeping them will provide.

## Quastions
1. What's the overall avarage per student?
2. What percent of students have grades higher than 70?
3. Is there any relationship between number of absences and final grade?
4. Do students who miss more than 5 days tends to have lower grades?

## Overview of the Dataset

|**Feature**|**Description**| **Type** | **Missing Values?** | 
|---------|------------|-------------|-------------|
|student_id	| Unique identifier for a student| String| No missing values in this feature|
|name	| Student name| String| No missing values in this feature|
|grades_math	| The mean grade value of math in the last 3 months | Integer| No missing values in this feature|
|grades_science	| The mean grade value of science in the last 3 months| Integer| No missing values in this feature|
|grades_history	| The mean grade value of history in the last 3 months| Integer| No missing values in this feature|
|grades_english	| The mean grade value of english in the last 3 months| Integer| No missing values in this feature|
|missed_days	| The mean missed days value of the student in the last 3 months| integer| No missing values in this feature|


# How data was obtained

The **workflow** to obtain the data was defined as shown in Fig. 1. A python script was built to read json files and build a pandas dataframe from them. Next, the data was pushed in snowflake so that business could perform the analysis.


![](workflow.png "Workflow")**Fig. 1**


The **relatioship** between students and missed_classes is described in Fig.2. students and missed_classes have a 1:1 relationship and joining  students and missed_classes a One Big Table was built.

![](modelagem.png "Relationship")**Fig.2**




