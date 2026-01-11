from pyspark.mllib.feature import HashingTF, IDF
from pyspark.ml import Pipeline
import os

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer, OneHotEncoder

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.types import DoubleType


from pyspark.ml.regression import RandomForestRegressor

from pyspark.ml.regression import LinearRegression

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql import SparkSession
import random
import argparse
from datetime import datetime
import csv

from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml import  PipelineModel
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor

def calculate_rmsle(result_df,labelCol="price", predictionCol="prediction"):
    import numpy as np
    
    label = np.array(result_df.select(labelCol).collect())
    prediction = np.array(result_df.select(predictionCol).collect())
    
    return np.sqrt(np.square(np.log(label + 1) - np.log(prediction + 1)).mean())

class Model_Pipeline():
  def __init__(self, save_dir,
               model_name, cross_validation, 
               seed,
               text_process_method = 'tf-idf',
              text_attr ='name_description',
              cat_attrs = [ 'second_category', 'third_category', 'shop_name','brand'],
              num_attrs = [ 'shop_reply_percentage']):
    
    self.save_dir = save_dir
    if not os.path.exists(self.save_dir):
      os.makedirs(self.save_dir)

    self.model_save_path = self.save_dir + 'model/'
    if not os.path.exists(self.model_save_path):
      os.makedirs(self.model_save_path)

    self.pipeline_save_path = self.save_dir + 'pipeline/'
    if not os.path.exists(self.pipeline_save_path):
      os.makedirs(self.pipeline_save_path)

    self.string_indexer_save_path = self.save_dir + 'string_indexer/'
    if not os.path.exists(self.string_indexer_save_path):
      os.makedirs(self.string_indexer_save_path)

    self.result_file_path = self.save_dir + 'result.csv'
    
    self.model_name = model_name
    self.cv = cross_validation
    self.seed = seed
    
    self.text_process_method = text_process_method
    self.min_doc_freq = 2
    self.n_gram = 1
        
    self.text_feature = text_attr
    self.cat_features = cat_attrs
    self.num_features = num_attrs

    self.pipeline_list = []
    self.pipeline = None
    

    self.feature_columns = []

    self.indexer =   StringIndexer(inputCols=[feature for feature in self.cat_features], outputCols= [ (feature + '_numeric' )for feature in self.cat_features])

  
  def build_pipeline(self):

    # Process string attributes with TF-IDF or CountVectorizer 
    tokenizer = Tokenizer(inputCol=self.text_feature, outputCol=self.text_feature + "_words")
    if self.text_process_method == 'tf-idf':
      hashing_tf = HashingTF(inputCol=self.text_feature + "_words", outputCol=self.text_feature + "_rawFeatures",numFeatures=500)
      idf_transformer = IDF(inputCol=self.text_feature + "_rawFeatures", outputCol=self.text_feature + '_features',minDocFreq=self.min_doc_freq)
      text_transformer = Pipeline(stages=[tokenizer,hashing_tf,idf_transformer])
      self.pipeline_list.append(text_transformer)
      self.feature_columns.append(self.text_feature + '_features')
    
    elif self.text_process_method == 'count-vectorizer':
      cv = CountVectorizer(inputCol= self.text_feature + "_words", outputCol=self.text_feature + '_features')
      text_transformer = Pipeline(stages=[tokenizer, cv])
      self.pipeline_list.append(text_transformer)
      self.feature_columns.append(self.text_feature + '_features')    

    # # Process categorical variables: one hot encoding
 
    # indexer = StringIndexer(inputCols=[feature for feature in self.cat_features], outputCols= [ (feature + '_numeric' )for feature in self.cat_features])
    oh_encoder = OneHotEncoder(inputCols = [(feature + '_numeric')for feature in self.cat_features], outputCols = [ (feature + '_onehot' )for feature in self.cat_features])
    categorical_transformer = Pipeline(stages=[oh_encoder])
    self.pipeline_list.append(categorical_transformer)

    for feature in self.cat_features:
      self.feature_columns.append(feature + '_onehot')

    #Process numeric attributes
    for feature in self.num_features:
      vectorAssembler = VectorAssembler(inputCols=[feature], outputCol=feature + '_unscaled')
      scaler_for_feature = MinMaxScaler(inputCol=feature + '_unscaled', outputCol=feature + '_features')
      numeric_transformer = Pipeline(stages = [vectorAssembler,scaler_for_feature])
      self.pipeline_list.append(numeric_transformer)
      self.feature_columns.append(feature + '_features')
    

    # Final feature assembler: assemble all chosen features
    feature_assembler = VectorAssembler(inputCols=self.feature_columns, outputCol= 'features')
    self.pipeline_list.append(feature_assembler)
    self.pipeline = Pipeline(stages = self.pipeline_list)

    return self.pipeline
  
  def process_input_df(self,df):
    processed_df = df
    processed_df = processed_df.fillna(0, subset=self.num_features)
    for feature in self.num_features:
      processed_df = processed_df.withColumn(feature, processed_df[feature].cast(DoubleType()))    
    processed_df = processed_df.withColumn('price', processed_df['price'].cast(DoubleType()))
    processed_df = processed_df.withColumn("log_price", F.log10(F.col('price')))    
    return processed_df

  def fit_transform(self, train_data):
    df = self.process_input_df(train_data)
    # Process categorical variables: one hot encoding 
    self.indexer = self.indexer.setHandleInvalid("skip").fit(df)
    df = self.indexer.transform(df)
    # Build data processing pipeline
    self.build_pipeline()
    self.pipeline = self.pipeline.fit(df)
    transformed_data = self.pipeline.transform(df)
    transformed_data.select('features').show()

    # Create a model
    if self.model_name == 'random_forest':
      self.model = RandomForestRegressor(featuresCol="features", labelCol="log_price", predictionCol= 'log_prediction', numTrees=100, seed= self.seed)

    if self.model_name == 'lr':
      self.model =  LinearRegression(featuresCol="features", labelCol="log_price",predictionCol= 'log_prediction')
    
    if self.model_name == 'gbt':
      self.model = GBTRegressor(featuresCol="features", labelCol='log_price', predictionCol='log_prediction')


    # result_df = self.model.transform(transformed_data).select(['price', 'log_prediction'])
    # result_df = result_df.withColumn("prediction", 10 ** F.col('log_prediction'))   
    # self.model.transform(transformed_data).select(['price', 'log_prediction']).show()
    self.save()

    return self.model
  

  def predict(self, input_data, load = True):
    if load:
      self.load()
    input_data = self.process_input_df(input_data)
    input_data = self.indexer.transform(input_data)
    transformed_data = self.pipeline.transform(input_data)

    result_df = self.model.transform(transformed_data).select(['price', 'log_prediction'])
    result_df = result_df.withColumn("prediction", 10 ** F.col('log_prediction'))  
    
    return result_df

  
  def hyper_tuning(self,train_data):
    df = self.process_input_df(train_data)
    self.indexer = self.indexer.setHandleInvalid("skip").fit(df)
    df = self.indexer.transform(df)
    # Build data processing pipeline
    self.build_pipeline()
    self.pipeline = self.pipeline.fit(df)
    transformed_data = self.pipeline.transform(df)
    # transformed_data.select('features').show()

    # Create a model
    if self.model_name == 'random_forest':
      self.model = RandomForestRegressor(featuresCol="features", labelCol="log_price", predictionCol= 'log_prediction',
                                         seed = self.seed)
      # Create ParamGrid for Cross Validation
      rfparamGrid = (ParamGridBuilder()
               .addGrid(self.model.maxDepth, [2, 5, 10])
               .addGrid(self.model.numTrees, [5, 20, 50])
             .build())
      rfevaluator = RegressionEvaluator(predictionCol="log_prediction", labelCol="log_price", metricName="r2")
      # Create 5-fold CrossValidator
      rfcv = CrossValidator(estimator = self.model,
                            estimatorParamMaps = rfparamGrid,
                            evaluator = rfevaluator,
                            numFolds = self.cv)
      
      self.model  = rfcv.fit(transformed_data)
      

    if self.model_name == 'lr':
      self.model =  LinearRegression(featuresCol="features", labelCol="log_price",predictionCol= 'log_prediction')
      
      lrevaluator = RegressionEvaluator(predictionCol="log_prediction", labelCol="log_price", metricName="r2")
      # Create 5-fold CrossValidator
      lrparamGrid = ParamGridBuilder()\
                  .addGrid(self.model.regParam, [0.1, 0.01]) \
                  .addGrid(self.model.elasticNetParam, [0.0, 0.5, 1.0])\
                  .build()

      lrcv = CrossValidator(estimator = self.model,
                            estimatorParamMaps = lrparamGrid,
                            evaluator = lrevaluator,
                            numFolds = self.cv)
      
      self.model  = lrcv.fit(transformed_data)

    if self.model_name == 'gbt':
      self.model = GBTRegressor(featuresCol="features", labelCol='log_price', predictionCol='log_prediction')
      
      gbtevaluator = RegressionEvaluator(predictionCol="log_prediction", labelCol="log_price", metricName="r2")
      # Create 5-fold CrossValidator
      gbtparamGrid = ParamGridBuilder()\
                  .addGrid(self.model.maxBins, [10, 20, 40]) \
                  .addGrid(self.model.maxDepth, [2, 5, 10])\
                  .build()

      gbtcv = CrossValidator(estimator = self.model,
                            estimatorParamMaps = gbtparamGrid,
                            evaluator = gbtevaluator,
                            numFolds = self.cv)
      
      self.model  = gbtcv.fit(transformed_data)
    self.save()
    print(self.model.bestModel)

    return self.model

  def evaluate(self, result_df):
    rmse = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
    rmse = rmse.evaluate(result_df)
    rmsle = calculate_rmsle(result_df)
    r2 = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
    r2 = r2.evaluate(result_df)
    current_time = datetime.now()
    # print(current_time)
    header = ['Date time','RMSE', "RMSLE", 'R2'] 
        
    # data rows of csv file 
    row = [current_time,rmse,rmsle,r2]
    with open(self.result_file_path, 'a+', encoding='UTF8', newline='') as f:
      writer = csv.writer(f)
      if os.stat(self.result_file_path).st_size == 0:
        # write the header
        writer.writerow(header)

      # write the data
      writer.writerow(row)
    return rmse, rmsle, r2

  def save(self):
    print("---Saving model to checkpoint---")
    if os.path.exists(self.model_save_path):
      if self.cv == 0:
        self.model.write().overwrite().save(self.model_save_path)
      else:
        self.model.bestModel.write().overwrite().save(self.model_save_path)
    
    else:
      if self.cv == 0:
        self.model.save(self.model_save_path)
      else:
        self.model.bestModel.save(self.model_save_path)


    if os.path.exists(self.pipeline_save_path):
      self.pipeline.write().overwrite().save(self.pipeline_save_path)
    else:  
      self.pipeline.save(self.pipeline_save_path)

    if os.path.exists(self.string_indexer_save_path):
      self.indexer.write().overwrite().save(self.string_indexer_save_path)
    else:  
      self.indexer.save(self.string_indexer_save_path)

  def load(self):
    print("---Load the trained model from checkpoint---")
    
    if self.model_name == 'random_forest':
      self.model = RandomForestRegressor.load(self.model_save_path)

    if self.model_name == 'lr':
      self.model = LinearRegressionModel.load(self.model_save_path)
    
    if self.model_name == 'gbt':
      self.model = GBTRegressor.load(self.model_save_path)

    self.build_pipeline()

    # self.model.load(self.model_save_path)
    # self.pipeline.load(self.pipeline_save_path)
    self.pipeline = PipelineModel.load(self.pipeline_save_path)
    self.indexer = StringIndexerModel.load(self.string_indexer_save_path)


if __name__ == '__main__':
    
    # Instantiate the parser
    parser = argparse.ArgumentParser(description='Model trainer, saver and evaluator')

    parser.add_argument('--model_name', type=str,
                        help='The name of the machine learning model: lr (linear regression), random_forest, \
                          gbt (Gradient Boosting Trees)', default = 'lr')
    parser.add_argument("--mode",type=str, help='Mode of running (either train or inference)', \
                        default='train')
    parser.add_argument('--save_dir', type=str,
                        help='Directory to save model and pipeline checkpoints', default = '../checkpoint/')
    parser.add_argument('--train_csv_path', type=str,
                        help='Path of the csv file used for training and testing', 
                        default = '../data/model_match_col.csv')
    parser.add_argument('--cross_validation', type = int,
                        help='Number of cross validation folds (0 for no cross validation)', default=5)
    parser.add_argument('--hyperparam_tuning', type = bool,
                        help='Whether to tune hyperparameters or not', default=True)
    parser.add_argument('--data_test_ratio', type = float,
                        help='Percentage of data used for testing', default=0.2)
    parser.add_argument('--seed', type = int,
                        help='Random seed', default=42)


    args = parser.parse_args()


    model_mame = args.model_name
    mode = args.mode
    save_dir = args.save_dir
    train_csv_path = args.train_csv_path
    cross_validation = args.cross_validation
    hyperparam_tuning = args.hyperparam_tuning
    test_ratio = args.data_test_ratio
    train_ratio  = 1 - test_ratio
    seed = args.seed


    random.seed(seed)
    np.random.seed(seed)

    # from pyspark import SparkContext, SparkConf
    # conf_spark = SparkConf().set("spark.driver.host", "127.0.0.1")
    #Create PySpark SparkSession
    spark = SparkSession.builder \
        .appName("BA Model") \
        .getOrCreate()
    
    df = spark.read.format("csv").option('header',"true").load(train_csv_path)    
    df.printSchema()


    print("Created model")
    model_pipeline = Model_Pipeline(save_dir=save_dir, model_name=model_mame, cross_validation=cross_validation, seed = seed)

    if mode == 'train':
      print("Split data with ratio {}/{}".format(train_ratio,test_ratio))
      train_df,  test_df = df.randomSplit([train_ratio,test_ratio], seed=seed)

      # Train model
      if hyperparam_tuning == False:
        print("Training model with no hyper-parameter tuning and cross validation")
        model_pipeline.fit_transform(train_df)
      else:
        print("Training model with hyper-parameter tuning and cross validation with {} folds".format(cross_validation))
        model_pipeline.hyper_tuning(train_df)
      
      print("Finished training the model")
      print("Evaluating on the test set")
    
    elif mode == 'inference':
      test_df = df
    
    test_df.show()

    result_df = model_pipeline.predict(test_df)
    model_pipeline.evaluate(result_df)
    
    # save result of prediction in prediction csv
    result_df = result_df.toPandas()
    result_df.to_csv(save_dir + "inference.csv",index=False, columns=['price', 'prediction'])
    


    #Create PySpark DataFrame from Pandas
    # spark_df = spark.read.format("csv").option('header',"true").load('data/ba_data_final.csv')

    # spark_df.printSchema()
    # spark_df.show()
