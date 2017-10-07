# spark-ml-prediction

**Problem Statement**

Use data mining to predict sightings of the Red-winged Blackbird (Agelaius phoeniceus) in each birding session as accurately as possible.

**Methodology**

The Project uses Spark MLlib and Random Forest Classification algorithm to train a model using labeled data set (8 GB) and to predict the sightings of Red-winged Blackbird in unlabeled data set (877 MB). Random Forests are ensembles of decision trees. 
As ensemble technique is used , project goal is to get a good bias while training specific decision tree and to reduce the risk of overfitting and achieve good variance by combining many of these decision trees.
For model training and validation, the full labeled data is partitioned into training (70%) and validation (30%).

To achieve good bias, we have analyzed the impact of many model parameters that are as follows:
- number of trees
- strategy for selecting subset of features
- maximum depth of the tree
- features categorization
- number of bins for discretizing continuous features
- amount of memory to be used for collecting sufficient statistics and  parameters to cache the model
- relevant features from dataset which can help in better prediction.

The in-build capabilities in spark MLlib such as RDD, parallel iterative computation and tunable parameters provided helped in building
scalable solution.

**Results**

Once a good number of runs were performed varying the features, tuning MLlib parameters for training model, we were able to achieve satisfactory training accuracy- 86%
