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

package ml;

// $example on$
import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

/**
 * Java example for simple text document 'Pipeline'.
 */
public class JavaPipelineExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("JavaPipelineExample")
      .getOrCreate();

    // $example on$
    // Prepare training documents, which are labeled.
    Dataset<Row> training = spark.createDataFrame(Arrays.asList(
      new JavaLabeledDocument(0L, "a b c d e spark", 1.0),
      new JavaLabeledDocument(1L, "b d", 0.0),
      new JavaLabeledDocument(2L, "spark f g h", 1.0),
      new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0)
    ), JavaLabeledDocument.class);

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    Tokenizer tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words");
    HashingTF hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol())
      .setOutputCol("features");
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001);
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

    // Fit the pipeline to training documents.
    PipelineModel model = pipeline.fit(training);

    Dataset<Row> trainPredict = model.transform(training);
    System.out.println("\n====== Train Result:");
    for (Row r : model.transform(training).select("id", "text", "words", "features", "probability", "prediction", "label").collectAsList()) {
      System.out.println("(" + r.get(0) + ", " + r.get(1) + ", " + r.get(2) + ", " + r.get(3) + ") \n\t--> prob=" + r.get(4)
              + ", prediction=" + r.get(5) + ", label=" + r.getAs("label"));
    }
    System.out.println("\n======");

    // Prepare test documents, which are unlabeled.
    Dataset<Row> test = spark.createDataFrame(Arrays.asList(
      new JavaDocument(4L, "spark i j k"),
      new JavaDocument(5L, "l m n"),
      new JavaDocument(6L, "spark hadoop spark"),
      new JavaDocument(7L, "apache hadoop")
    ), JavaDocument.class);

    // Make predictions on test documents.
    Dataset<Row> predictions = model.transform(test);
    System.out.println("\n+++++ Predict Result:");
    for (Row r : predictions.select("id", "text", "words", "features", "probability", "prediction").collectAsList()) {
      System.out.println("(" + r.getAs("id") + ", " + r.getAs("text") + ", " + r.getAs("features") + ") \n\t--> prob=" + r.getAs("probability")
        + ", prediction=" + r.getAs("prediction"));
    }
    System.out.println("\n+++++");
    // $example off$

    spark.stop();
  }
}
