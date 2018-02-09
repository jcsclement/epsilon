# Databricks notebook source
import sys
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.datasets import load_files
from sklearn.model_selection import train_test_split
from sklearn import metrics

# COMMAND ----------

# the training data folder must be passed as first argument
dataset = load_files('/dbfs/mnt/epsilon-reviews', shuffle=False)

# spark_df = spark.csv.options(delimiter = '').load("/mnt/epsilon-reviews*")
# dataset = spark_df.toPandas()

print("n_samples: %d" % len(dataset.data))

# split the dataset in training and test set:
docs_train, docs_test, y_train, y_test = train_test_split(dataset.data, dataset.target, test_size=0.25, random_state=100)

# COMMAND ----------

# TASK: Build a vectorizer / classifier pipeline that filters out tokens
# that are too rare or too frequent
pipeline = Pipeline([('vect', TfidfVectorizer(min_df=3, max_df=0.95)),('clf', LinearSVC(C=1000)),])

# COMMAND ----------

# TASK: Build a grid search to find out whether unigrams or bigrams are
# more useful.
# Fit the pipeline on the training set using grid search for the parameters
parameters = {
    'vect__ngram_range': [(1, 1), (1, 2)],
}
grid_search = GridSearchCV(pipeline, parameters, n_jobs=-1)
grid_search.fit(docs_train, y_train)

# COMMAND ----------

# TASK: print the mean and std for each candidate along with the parameter
# settings for all the candidates explored by grid search.
n_candidates = len(grid_search.cv_results_['params'])
for i in range(n_candidates):
    print(i, 'params - %s; mean - %0.2f; std - %0.2f'
             % (grid_search.cv_results_['params'][i],
                grid_search.cv_results_['mean_test_score'][i],
                grid_search.cv_results_['std_test_score'][i]))


# COMMAND ----------

# TASK: Predict the outcome on the testing set and store it in a variable
# named y_predicted
y_predicted = grid_search.predict(docs_test)

# COMMAND ----------

# Print the classification report
print(metrics.classification_report(y_test, y_predicted,
                                    target_names=dataset.target_names))

# Print and plot the confusion matrix
cm = metrics.confusion_matrix(y_test, y_predicted)
print(cm)

# COMMAND ----------

# save model
from sklearn.externals import joblib
joblib.dump(grid_search, '/dbfs/mnt/epsilon-reviews/predictSentiment.pkl')

# COMMAND ----------

# test model
grid_search.predict(["This is an excellent product", "Hi this insurance product sucks", "you guys are simply fantastic", "I lost my credit card and I feel lost"])

# COMMAND ----------

type(docs_test)

# COMMAND ----------

