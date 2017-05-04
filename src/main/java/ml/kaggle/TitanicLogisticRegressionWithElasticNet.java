/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ml.kaggle;

// $example on$
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

public class TitanicLogisticRegressionWithElasticNet {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLogisticRegressionWithElasticNetExample")
      .getOrCreate();

    // $example on$
    // Load training data
    Dataset<Row> training = spark.read().format("csv").option("header", true).option("inferSchema", true)
      .load("/home/paul/share/mySparkJavaApiLearning/src/main/resources/kaggle/Titanic/gen_LR_train_data.csv");
    training.printSchema();
    training.show();

    String origStr = "SibSp,Parch,Cabin_No,Cabin_Yes,Embarked_C,Embarked_Q,Embarked_S,Sex_female,Sex_male,Pclass_1,Pclass_2,Pclass_3,Age_scaled,Fare_scaled";
    String[] ayyOrig = origStr.split(",");
    VectorAssembler vectorSlicer = new VectorAssembler()
            .setInputCols(ayyOrig).setOutputCol("features");
    Dataset<Row> feaTrain = vectorSlicer.transform(training);
    feaTrain.printSchema();
    feaTrain.show();
    feaTrain = feaTrain.select("features", "Survived");
    feaTrain.printSchema();
    feaTrain.show();

    LogisticRegression lr = new LogisticRegression()
      .setLabelCol("Survived")//Column SibSp must be of type org.apache.spark.ml.linalg.VectorUDT
      //.setFeaturesCol("SibSp Parch Cabin_No Cabin_Yes Embarked_C Embarked_Q Embarked_S Sex_female Sex_male Pclass_1 Pclass_2 Pclass_3 Age_scaled Fare_scaled")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8);

    // Fit the model
    LogisticRegressionModel lrModel = lr.fit(feaTrain);

    // Print the coefficients and intercept for logistic regression
    System.out.println("\n---------- Binomial logistic regression's Coefficients: "
      + lrModel.coefficients() + "\nBinomial Intercept: " + lrModel.intercept());

    Dataset<Row> testData = spark.read().format("csv").option("header", true).option("inferSchema", true)
            .load("/home/paul/share/mySparkJavaApiLearning/src/main/resources/kaggle/Titanic/gen_LR_test_data.csv");
    Dataset<Row> feaTest = vectorSlicer.transform(testData);
    Dataset<Row> result = lrModel.transform(feaTest);
    result.write().mode("overwrite").csv("spark_LR_result");

    spark.stop();
  }
}
