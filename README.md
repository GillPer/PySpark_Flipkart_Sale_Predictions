# PySpark_Flipkart_Sale_Predictions

This project leverages PySpark to analyze and predict discount percentages for Flipkart products. By analyzing various product attributes, it provides insights into discount strategies, helping business stakeholders make data-driven decisions on sale pricing. Key functionalities include ETL processes, data preprocessing, and model training with evaluation metrics.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Installation and Setup](#installation-and-setup)
- [Dataset Overview](#dataset-overview)
- [Project Steps](#project-steps)
  - [1. Data Ingestion](#1-data-ingestion)
  - [2. Data Cleaning and Preparation](#2-data-cleaning-and-preparation)
  - [3. Feature Engineering](#3-feature-engineering)
  - [4. Model Building](#4-model-building)
  - [5. Evaluation](#5-evaluation)
  - [6. Results and Visualization](#6-results-and-visualization)
- [Future Work](#future-work)

---

## Project Overview
This project predicts the discount percentages on Flipkart products based on historical data. Using PySpark and machine learning models, we process, transform, and analyze the dataset to generate predictive insights on sale strategies. 

## Technologies Used
- **Python**: General scripting and data manipulation.
- **PySpark**: Data processing and machine learning.
- **Databricks**: Cloud-based Spark cluster setup and execution.
- **Matplotlib & Seaborn**: Visualization in local Jupyter Notebooks.
- **Tableau/Power BI**: Interactive data visualizations.

## Installation and Setup
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/flipkart-sale-prediction.git
   cd flipkart-sale-prediction

2. **Set Up PySpark on Databricks:**
- Create a new cluster on Databricks.
- Upload the dataset to DBFS (`/FileStore/train_data.csv`).
- Import the notebook (`PySpark_Flipkart_Sale_Predictions.ipynb`) to Databricks.
  
3. **Install Required Libraries:** Ensure necessary packages like `matplotlib`, `seaborn`, and `pandas` are available in your Databricks cluster.
   
## Dataset Overview
The dataset contains Flipkart product details, ratings, prices, and user feedback, which are used to predict the discount percentage. Key columns include:

- `title`, `maincateg`, `platform`
- `actprice1 (original price), `price1` (discounted price)
- `Rating`, `fulfilled1` (order fulfillment status)

## Project Steps
1. Data Ingestion
- Load the dataset using PySpark's DataFrame API.
- Check data schema and identify missing values or erroneous entries.

```bash
train_data = spark.read.csv('/dbfs/FileStore/train_data.csv', header=True, inferSchema=True)
```

2. Data Cleaning and Preparation
- Remove duplicates and handle missing values by either dropping or imputing them.
- Convert columns to appropriate data types (e.g., price1 and actprice1 to DoubleType for calculations).

```bash
train_data = train_data.na.drop()
```

3. Feature Engineering
- Create a new feature discount_percentage using actprice1 and price1.
- One-hot encode categorical variables (platform, maincateg).
- Normalize or scale features if necessary.

```bash
from pyspark.sql.functions import col
train_data = train_data.withColumn('discount_percentage', (col('actprice1') - col('price1')) / col('actprice1') * 100)
```

4. Model Building
- Split the data into training and validation sets.
- Use PySpark MLlib to create a regression model (e.g., Linear Regression).
- Train the model on training data.

```bash
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features', labelCol='discount_percentage')
model = lr.fit(training_set)
```

5. Evaluation
- Use metrics like RMSE (Root Mean Squared Error) and R2 to evaluate model performance on the validation set.

```bash
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(labelCol="discount_percentage", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
```

6. Results and Visualization
- Visualize feature importance, model predictions, and actual vs. predicted values using Matplotlib or Seaborn in Jupyter Notebooks.
- Use Tableau or Power BI for interactive data dashboards and insights.
