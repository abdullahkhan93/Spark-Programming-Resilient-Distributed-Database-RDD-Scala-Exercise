package questions

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{LogisticRegression,LogisticRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType}

import org.apache.spark.sql._


/**
* GamingProcessor is used to predict if the user is a subscriber.
* You can find the data files from /resources/gaming_data.
* Data schema is https://github.com/cloudwicklabs/generator/wiki/OSGE-Schema
* (first table)
*
* Use Spark's machine learning library mllib's logistic regression algorithm.
* https://spark.apache.org/docs/latest/ml-classification-regression.html#binomial-logistic-regression
*
* Use these features for training your model:
*   - gender 
*   - age
*   - country
*   - friend_count
*   - lifetime
*   - citygame_played
*   - pictionarygame_played
*   - scramblegame_played
*   - snipergame_played
*
*   -paid_subscriber(this is the feature to predict)
*
* The data contains categorical features, so you need to
* change them accordingly.
* https://spark.apache.org/docs/latest/ml-features.html
*
*
*
*/
class GamingProcessor() {


  // these parameters can be changed
  val spark = SparkSession.builder
  .master("local")
  .appName("gaming")
  .config("spark.driver.memory", "5g")
  .config("spark.executor.memory", "2g")
  .getOrCreate()

  import spark.implicits._


  /**
  * convert creates a dataframe, removes unnecessary colums and converts the rest to right format. 
  * Data schema:
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *
  * @param path to file
  * @return converted DataFrame
  */
  def convert(path: String): DataFrame = {
    var file = spark.sparkContext.textFile(path)
    val tokens = file.map(line => line.split(','))
    var records = tokens.map(x => 
    Row(x(3).trim match {
      case "male" => 1.0
      case _ => 0.0
      },
      x(4).toDouble,
      x(6).toString,
      x(8).toDouble,
      x(9).toDouble,
      x(10).toDouble, 
      x(11).toDouble,
      x(12).toDouble,
      x(13).toDouble,
      x(15).trim match {
      case "yes" => 1.0
      case _ => 0.0
      }))

    val schema = StructType(
        Array(
          StructField("gender", DoubleType),
          StructField("age", DoubleType),
          StructField("country", StringType),
          StructField("friend_count", DoubleType),
          StructField("lifetime", DoubleType),
          StructField("game1", DoubleType),
          StructField("game2", DoubleType),
          StructField("game3", DoubleType),
          StructField("game4", DoubleType),
          StructField("paid_customer", DoubleType)
          )
    )
    return spark.createDataFrame(records, schema)
  }

  /**
  * indexer converts categorical features into doubles.
  * https://spark.apache.org/docs/latest/ml-features.html
  * 'country' is the only categorical feature.
  * After these modifications schema should be:
  *
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *   - country_index: Double
  *
  * @param df DataFrame
  * @return Dataframe
  */
  def indexer(df: DataFrame): DataFrame = {
  	val index = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("country_index")

    return index.fit(df).transform(df)
  }

  /**
  * Combine features into one vector. Most mllib algorithms require this step
  * https://spark.apache.org/docs/latest/ml-features.html#vectorassembler
  * Column name should be 'features'
  *
  * @param Dataframe that is transformed using indexer
  * @return Dataframe
  */
  def featureAssembler(df: DataFrame): DataFrame = {
  	val assembler = new VectorAssembler()
     .setInputCols(Array("gender", "age", "friend_count", "lifetime", "game1", "game2", "game3", "game4", "paid_customer", "country_index"))
     .setOutputCol("features")
    val transDF = assembler.transform(df)
    return transDF
  }

  /**
  * To improve performance data also need to be standardized, so use
  * https://spark.apache.org/docs/latest/ml-features.html#standardscaler
  *
  * @param Dataframe that is transformed using featureAssembler
  * @param  name of the scaled feature vector (output column name)
  * @return Dataframe
  */
  def scaler(df: DataFrame, outputColName: String): DataFrame = {
  	val scaler = new StandardScaler()
  	 .setInputCol("features")
     .setOutputCol(outputColName)
     .setWithStd(true)
     .setWithMean(true)
     val scalerModel = scaler.fit(df)
     val scaledDF = scalerModel.transform(df)
    return scaledDF
  }

  /**
  * createModel creates a logistic regression model
  * When training, 5 iterations should be enough.
  *
  * @param transformed dataframe
  * @param featuresCol name of the features columns
  * @param labelCol name of the label col (paid_customer)
  * @param predCol name of the prediction col
  * @return trained LogisticRegressionModel
  */
  def createModel(training: DataFrame, featuresCol: String, labelCol: String, predCol: String): LogisticRegressionModel = {
    val lr = new LogisticRegression()
     .setMaxIter(5)
     .setRegParam(0.3)
     .setElasticNetParam(0.8)
     .setFeaturesCol(featuresCol)
     .setLabelCol(labelCol)
     .setPredictionCol(predCol)
    val modelLR1 = lr.fit(training)
    return modelLR1
  }



  /**
  * Given a transformed and normalized dataset
  * this method predicts if the customer is going to
  * subscribe to the service.
  *
  * @param model trained logistic regression model
  * @param dataToPredict normalized data for prediction
  * @return DataFrame predicted scores (1.0 == yes, 0.0 == no)
  */
  def predict(model: LogisticRegressionModel, dataToPredict: DataFrame): DataFrame = {
    val prediction = model.transform(dataToPredict)
    val lp = prediction.select("label", "prediction")
  	return lp 
  }

}
/**
*
*  Change the student id
*/
object GamingProcessor {
    val studentId = "662011"
}
