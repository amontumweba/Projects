# -*- coding: utf-8 -*-

# Import important libralies and models

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pylab import *

import sklearn
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_predict

from sklearn import metrics
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score, recall_score
from sklearn import neighbors
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split


"""Setting the figure dimensions"""

%matplotlib inline
rcParams['figure.figsize'] = 10, 6

""" 
Importing seaborn library and setting the figure sytle
"""
import seaborn as sb
sb.set_style('whitegrid')


"""
Importing the dataset 
"""

df = pd.read_csv("../input/churn-modellingcsv/Churn_Modelling.csv")

"""
Checking that my target variable is binary
"""

sb.countplot(x = 'Exited', data = df, palette='hls')
plt.show()

"""
Checking for missing
"""
df.isnull().sum()

"""
As observed above, there are no missing values in all of the features
"""

"""
Getting the description of the dataset
"""
df.describe()


"""
Checking the information regarding the dataset to see whether variables have equal size
"""
df.info()

""" Getting unique count for each variable
"""
df.nunique()

"""
Dropping the irrelevant columns
"""

df = df.drop(['RowNumber', 'CustomerId', 'Surname'], axis = 1)

"""The main reason for dropping the above features is because;
* The rownumber attribute acts like a counter of records
* The customerid attribute acts as a unique identifier for a customer 
* the surname attribute as an entrie for the customer. 
* Therefore, they don't have any useful impact on the analysis 
 and thus dropping them from the dataset doesn't have any negative impact on the model
"""

"""
Checking the data types for each variable
"""
df.dtypes

"""
Checking for the percentage per category for the target column
"""
labels_perc = df.Exited.value_counts(normalize = True) * 100
labels_perc

"""The above results show that target label 0 has many records compared to the target label 1. For the label 0, there are about 79.63% whereas for the label 1, there are about 20.37%"""

""" 
Getting the correlation matrix of the target and other features
"""
df[df.columns].corr()

"""
Checking for independence between features using a heat map
"""
sb.heatmap(df.corr(), annot = True, fmt = ".2f")
plt.show()

"""
From the graph above,
* it the diagonal values are highly correlated since they are correlated with other
* balance feature is negatively correlated with NumOfProducts which means that as one is increasing,the other is decreasing.
"""

"""
Ploting the count plot for categorical to analyse how they are performing between 
churn and non churn customers
"""

Categorical_features = ['Geography', 'Gender', 'HasCrCard', 'IsActiveMember']

fig = plt.figure()
for i, cat_features in enumerate(Categorical_features):
    plt.subplot(2, 2, i+1)
    sb.countplot(x=cat_features, hue = 'Exited',data = df)
    fig.subplots_adjust(hspace=0.5, wspace=1)
    plt.title(f'{cat_features}')

"""
From the above plots;
* Germany has the highest proportion of churned customers, followed by France and then Spain
* France has the highest proportion of non churning customers, followed by Spain and then Germany
* Also, it is observed that females churn more than male
* Astonishingly, customers with credit cards have the highest churning rate
* Lastly, inactive customers churn more compared to active customers
"""

"""
Ploting the count plot for numerical to analyse how they are performing between 
churn and non churn customers
"""

Numerical_features = ['CreditScore', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'EstimatedSalary']

fig = plt.figure()
for i, num_features in enumerate(Numerical_features):
    plt.subplot(2, 3, i+1)
    sb.boxplot(y=num_features,x = 'Exited', hue = 'Exited',data = df)
    fig.subplots_adjust(hspace=0.5, wspace=1)
    plt.title(f'{num_features}')

"""
From the above plots;
* Looking at the credit score, non churned and churned customers have a small/no difference
* Looking at the age, old customers have a high level of churning rate compared to the young customers
* Looking at the tenure, customers who have spent more time with the bank are more likely to churn compared to those who have spent an average time with it.
* Looking at the balance, customers with higher balance are likely to leave which is not good for the bank
* Looking at number of products, number of products don't have much impact on the rate of churn
* And also, estimated salary has no any impact on the churn rate.
"""

"""
Converting categorical variables to a dummy indicators
for Gender
"""
label_encoder = LabelEncoder()
gender_cat = df['Gender']
gender_encoded = label_encoder.fit_transform(gender_cat)

"""
first 7 values
"""
gender_encoded[0:7]

"""
1 = male and 0 = female
"""
gender_DF = pd.DataFrame(gender_encoded, columns=['male_gender'])
gender_DF.head()

"""
For Geograph using one hot encoder
"""
geography_cat = df['Geography']
geography_encoded = label_encoder.fit_transform(geography_cat)

"""
Printing the first 100 values
"""
geography_encoded[0:100]

binary_encoder = OneHotEncoder(categories='auto')
geography_1hot = binary_encoder.fit_transform(geography_encoded.reshape(-1, 1))
geography_1hot_mat = geography_1hot.toarray()
geography_DF = pd.DataFrame(geography_1hot_mat, columns=['France', 'Spain', 'Germany'])
geography_DF.head()

"""The above code, initializes the onehot encoder function, changes the shape of the encoder, makes an array and changes to a dataframe"""

"""
Printing the first 5 rows
"""
geography_DF.head()

"""
Dropping the original Gender and Geography attributes
"""
df = df.drop(['Gender', 'Geography'], axis=1)

"""
Concatinating the dummy variables to the original dataset
"""
df_dummy = pd.concat([df, gender_DF, geography_DF], axis=1, verify_integrity=True)

"""
Printing the first 5 rows
"""
df_dummy.head()

"""
Checking for the independence between features with dummy values using a heat map
"""
sb.heatmap(df_dummy.corr(), annot = True, fmt = ".2f")
plt.show()

"""
Feature Engineering

I would to explore and get more variables I think they can have more impact on the model; below are the variable I came up with;

1. The second variable I came up with is the ratio that puts into account the balance and the estimated salary. This helps see whether customers with high balance ratio will churn or viceversa
"""

df_dummy['Balance_Estimate_Salary_Ratio'] = df_dummy['Balance']/(df_dummy['EstimatedSalary'])

"""
Normalizing the Credit score, Age, Balance, EstimatedSalary, Balance_Estimated_Salary_Ratio

"""
df_dummy.CreditScore = (df_dummy.CreditScore - df_dummy.CreditScore.min())/(df_dummy.CreditScore.max() - df_dummy.CreditScore.min())

df_dummy.Age = (df_dummy.Age - df_dummy.Age.min())/(df_dummy.Age.max() - df_dummy.Age.min())

df_dummy.Balance = (df_dummy.Balance - df_dummy.Balance.min())/(df_dummy.Balance.max() - df_dummy.Balance.min())

df_dummy.EstimatedSalary = (df_dummy.EstimatedSalary - df_dummy.EstimatedSalary.min())/(df_dummy.EstimatedSalary.max() - \
                                                                                     df_dummy.EstimatedSalary.min())

df_dummy.Balance_Estimate_Salary_Ratio = (df_dummy.Balance_Estimate_Salary_Ratio - df_dummy.Balance_Estimate_Salary_Ratio.min())/ \
(df_dummy.Balance_Estimate_Salary_Ratio.max() - df_dummy.Balance_Estimate_Salary_Ratio.min())

"""
The main reason for normalizing is because, there could be some outliers and normalizing them could reduce the 
effect of outliers to the model and most models work well with small values

###Building Predictive Model

Because I need classification model, I will try using different models to choose one with highest performance. And also putting into consideration that my labels are not balanced, trying out different models is better. Below are the models I will use;
1. Logistic Regression
2. Random Forest
3. K-Nearest Neighbor
4. Decision Tree
5. AdaBoost
6. Gradient Boosting
"""

"""
Separating the target column which contains answer for row from other attributes
"""
X = df_dummy.drop(['Exited'], axis=1)
y = df_dummy.Exited

"""
Splitting the dataset into training and testing set 
"""
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.30, random_state = 25)

"""
Printing the shapes of X_train and y_train datasets
"""
print(X_train.shape)
print(y_train.shape)

"""
As shown above, the training data is splitted. 30% of the training data will be used to check the training accuracy of the model and the remaining 70% will be used for the actual training purposes.

### Logistic Regression
"""

"""
Initializing Logistic Regression model
"""
LogReg = LogisticRegression(C=100, class_weight=None, dual=False, fit_intercept=True,intercept_scaling=1, max_iter=1000,multi_class='auto',n_jobs=None, 
                            penalty='l2', random_state=None, solver='lbfgs', tol=1e-05, verbose=0, 
                            warm_start=False)
"""
Fitting the model with the training data
"""
LogReg.fit(X_train, y_train)

"""
Predicting the response for the dataset
"""
yLog_pred = LogReg.predict(X_test)

"""
Model Evaluation 
"""

print(classification_report(y_test, yLog_pred))

"""
K-fold cross-validation and confusion matrices
"""
y_train_pred = cross_val_predict(LogReg, X_train, y_train, cv=5)
confusion_matrix(y_train, y_train_pred)

"""
Computing the accuracy of the model
"""
LogReg.score(X_test, y_test)

"""### KNN"""

"""
Initialization of the KNN model
"""
clf = neighbors.KNeighborsClassifier(n_neighbors=5, weights='uniform', algorithm='auto', leaf_size=30, p=2, 
                                    metric='minkowski', metric_params=None)
"""
Fitting the model with training data
"""
clf.fit(X_train, y_train)

"""
Evaluate model predictions
"""
yKNN_pred = clf.predict(X_test)
y_expect = y_test

print(classification_report(y_expect, yKNN_pred))

"""
Computing model accuracy
"""
clf.score(X_test, y_test)

"""### Random Forest"""

"""
Initialization of the model
"""
classifier = RandomForestClassifier(n_estimators = 200, random_state = 0)
y_train_array = np.ravel(y_train)

"""
Fitting the model with training data
"""
classifier.fit(X_train, y_train_array)


"""
Predicting the response for the dataset
"""
yRand_pred = classifier.predict(X_test)

"""
Evaluating model predictions
"""
print(classification_report(y_test, yRand_pred))

"""
Computing the model accuracy
"""
classifier.score(X_test, y_test)

"""
Comparing the y_test and model predictions
"""
y_test_array = np.ravel(y_test)
print(y_test_array)

print(yRand_pred)

"""As it can be observed from above, the model is not predicting well on some values

### AdaBoost model
"""

"""
Initialization of the AdaBoost model
"""
adaBoost = AdaBoostClassifier(base_estimator= None, n_estimators=200, learning_rate= 1.0)

"""
Fitting the model with the training data
"""
adaBoost.fit(X_train, y_train)

"""
Predicting the response for the dataset
"""
yAda_pred = adaBoost.predict(X_test)

"""
Evaluating model predictions
"""
print(classification_report(y_test, yAda_pred))

"""
Computing the model accuracy
"""
adaBoost.score(X_test, y_test)

"""### Decision Tree"""

"""
Create Decision Tree classifier object
"""
treeClf = DecisionTreeClassifier(criterion='entropy', max_depth = 3)

"""
Train Decision Tree Classifier
"""
treeClf.fit(X_train, y_train)

"""
Predicting the response for the dataset
"""
ytree_pred = treeClf.predict(X_test)

"""
Evaluating model predictions
"""
print(classification_report(y_test, ytree_pred))

"""
Computing the model acuracy
"""
treeClf.score(X_test, y_test)

"""### Gradient Boost"""

"""
Initialization of Gradient Boosting model
"""
gdBoost = GradientBoostingClassifier(loss = 'deviance', n_estimators = 200)

"""
Fitting the model with the training data
"""
gdBoost.fit(X_train, y_train)

"""
Predicting the response for the dataset
"""
gd_pred = gdBoost.predict(X_test)

"""
Evaluating model predictions
"""
print(classification_report(y_test, gd_pred))

"""
Computing the accuracy of the model
"""
gdBoost.score(X_test, y_test)

"""
Comparing y_test and gradient boost predicted values
"""
print(y_test_array)

print(gd_pred)

"""From the above results, it is observed that, the model is not performing well on some values. 
* However, I believe that by feeding with more and balanced data can make it perform highly better

### Conclusion

To evaluate the performance of my models, I put into consideration different metrics which include theses ones below;
* **Recall**: When the actual value is positive, how often is the prediction correct?
* **Precision** : When a positive value is predicted, how often is the prediction correct?
* **Accuracy** : The ratio of correctly predicted observation to the total observations
* The main reason for this is mainly because, my dataset has unbalanced labels **(True (1) and False (0))**.
* Therefore, putting into consideration those three metrics mentioned above, it is observed that, **Gradient Boosting model** performs better than others followed by **Random Forest** and then **AdaBoost model**.
* **Decision tree** and **Logistic models** didn't perform well

* In addition, from the analysis made, it was observed that; 
* Germany has the highest proportion of churned customers, followed by France and then Spain
* France has the highest proportion of non churning customers, followed by Spain and then Germany
* Also, it is observed that females churn more than males
* Astonishingly, customers with credit cards have the highest churning rate
* Lastly, inactive customers churn more compared to active customers

* More to that;
* Looking at the credit score, non churned and churned customers have a small/no difference
* Looking at the age, old customers have a high level of churning rate compared to the young customers
* Looking at the tenure, customers who have spent more time with the bank are more likely to churn compared to those who have spent an average time with it.
* Looking at the balance, customers with higher balance are likely to leave which is not good for the bank
* Looking at number of products, number of products don't have much impact on the rate of churn
* And also, estimated salary has no any impact on the churn rate.

* Updating the Gradient Boosting, Random Forest and AdaBoost models with more and balanced data would make them perform better
"""

