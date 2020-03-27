# Chapter 8. ML: classification and clustering


Table of Contents: 

> 8.1. Spark ML library </br>
> 8.2. Logistic regression </br>
> 8.3. Decision trees and random forests </br>
> 8.4. Using k-means clustering </br>
> 8.5. Summary </br>

Documents: 

> [http://spark.apache.org/docs/latest/ml-guide.html](http://spark.apache.org/docs/latest/ml-guide.html)</br>
> [http://spark.apache.org/examples.html](http://spark.apache.org/examples.html)
> [https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples)

## 8.1 Spark ML Library

Spark “Pipelines and Parameters” design document: [http://mng.bz/22lY](http://mng.bz/22lY)</br>

### 8.1.1. Estimators, transformers, and evaluators

<b>Transformers</b> (no, not those Transformers) is used to implement machine learning components. 

> The main method</b> is `transform`, takes a `DataFrame` and `optional set` as parameters</br>

<b>Estimators</b> produce `transformers` by fitting a dataset. 

> The main method</b> is `fit`, takes a `DataFrame` and `optional set` as parameters</br>

<b>Evaluators</b> evaluate the performance of a model based on a single metric such as `MSE` and `R2`</br>

### 8.1.2. ML parameters

<b>Param</b>: specifies parameter type

> including `name`, `class type`, `decription`, `validating function`, `optional default value` </br>

<b>ParamPair</b>: contains a parameter type (a `Param` object) and its value </br>

<b>ParamMap</b>: contains a set of `ParamPair` object

> `ParamPair` or `ParamMap` is passed to `fit` or `transform` methods to set the parameters 

### 8.1.3. ML pipelines

use `Pipeline`, `PipelineModel` could parameterize the model training, each time you add or adjust some steps in the training process, only need to change the parameter rather than implementing a new data flow.

> 使用PipeLine来封装模型各个步骤，并用参数控制各步骤的行为以及是否开启，这样就不需要因为某些步骤的变化而重复开发 

## 8.2 Logistic regression

### 8.2.1. Binary logistic regression model

Introducting the Logistic Regression algorithm, and feature processing steps such as `one-hot encoding`

> 介绍LR模型的原理，以及`OneHot Encoding`之类的特征处理步骤

### 8.2.2. Preparing data to use logistic regression in Spark

Data set: [http://archive.ics.uci.edu/ml/datasets/Adult](http://archive.ics.uci.edu/ml/datasets/Adult) <br/>

> Features: a person’s sex, age, education, marital status, race, native country, ...<br/>
> Target: to predict whether a person earns more or less than $50,000 per year <br/>

download data set: 

> suggest download the dataset from the git repo rather than uci.edu

dealing with missing values, options are as belows: 

> * If a lot of data is missing from a column, you can remove the entire column</br>
> * remove the individual examples (rows) from the dataset if they contain too many missing values</br>
> * set the missing data to the most common value in the column, e.g. the most frequent one</br>
> * train a separate classification or regression model and use it to predict the missing values</br>
> * ...

several Spark APIs can be used for dealing with these missing values, such as:  `replace`, `DataFrameNaFunctions`, `fill`, `drop`, ..., reference is in [http://mng.bz/X3Zg](http://mng.bz/X3Zg) 



code is as below, including: </br>
1. load data </br>
2. examine the data by loading it into a DataFrame </br>
3. examine missing values </br>
4. missing data imputation </br>
5. use StringIndexer to deal with categorical features </br>
6. use OneHotEncoder to deal with categorical features </br>
7. merge data with VectorAssembler</br>

> `StringIndexerModel` also adds metadata to the columns it transforms. This metadata contains information about the type of values a column contains (binary, nominal, numeric)</br>
> `VectorAssembler` also adds metadata about features it assembles

~~~scala
import spark.implicits._

// 1. load data
// 将所有数值列转成double，其余不能转的列保持string类型
val census_raw = sc.textFile("first-edition/ch08/adult.raw", 4)
                                .map(x => x.split(",")
                                .map(_.trim))
                                .map(row => row.map(x => try { x.toDouble } catch { case _ : Throwable => x })) 

// 2. examine the data by loading it into a DataFrame
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
val adultschema = StructType(Array(
    StructField("age",DoubleType,true),
    StructField("workclass",StringType,true),
    StructField("fnlwgt",DoubleType,true),
    StructField("education",StringType,true),
    StructField("marital_status",StringType,true),
    StructField("occupation",StringType,true),
    StructField("relationship",StringType,true),
    StructField("race",StringType,true),
    StructField("sex",StringType,true),
    StructField("capital_gain",DoubleType,true),
    StructField("capital_loss",DoubleType,true),
    StructField("hours_per_week",DoubleType,true),
    StructField("native_country",StringType,true),
    StructField("income",StringType,true)
))
import org.apache.spark.sql.Row
val dfraw = spark.createDataFrame(census_raw.map(Row.fromSeq(_)), adultschema)
dfraw.show()

// 3. examing missing values 
// 缺失值最多的3列: workclass, occupation, native_country
// 检查workclass列各个值的样本数量，?表示缺失值，Private是使用最频繁的值
// 使用通用的方法检查另外2列：occupation最频繁的取值是Prof-specialty；native_country最频繁取值是United-States
dfraw.groupBy(dfraw("workclass")).count().rdd.foreach(println)
// [?,2799] 
// [Self-emp-not-inc,3862]
// [Never-worked,10]
// [Self-emp-inc,1695]
// [Federal-gov,1432]
// [State-gov,1981]
// [Local-gov,3136]
// [Private,33906]
// [Without-pay,21]


//4. Missing data imputation
//用上一步找出的最频繁值来替换缺失值，更多的方法及API见http://mng.bz/X3Zg
val dfrawrp = dfraw.na.replace(Array("workclass"), Map("?" -> "Private"))
val dfrawrpl = dfrawrp.na.replace(Array("occupation"), Map("?" -> "Prof-specialty"))
val dfrawnona = dfrawrpl.na.replace(Array("native_country"), Map("?" -> "United-States"))

//5. use StringIndexer to deal with categorical features
import org.apache.spark.sql.DataFrame

def indexStringColumns(df:DataFrame, cols:Array[String]):DataFrame = {
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.feature.StringIndexerModel

    // variable newdf will be updated several times
    var newdf = df
    for(c <- cols) {
        // 为每个类别特征训练一个StringIndexer    
        val si = new StringIndexer().setInputCol(c).setOutputCol(c+"-num")
        val sm:StringIndexerModel = si.fit(newdf)
        // 用StringIndexer生成新特征，替换旧特征
        newdf = sm.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-num", c)
    }
    newdf
}
val dfnumeric = indexStringColumns(dfrawnona, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"))

//6. use OneHotEncoder to deal with categorical features
def oneHotEncodeColumns(df:DataFrame, cols:Array[String]):DataFrame = {
    import org.apache.spark.ml.feature.OneHotEncoder
    var newdf = df
    for(c <- cols) {
        // 为每个类别特征设置一个OneHotEncoder，转换并替换原特征 
        val onehotenc = new OneHotEncoder().setInputCol(c)
        onehotenc.setOutputCol(c+"-onehot").setDropLast(false)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
    }
    newdf
}
val dfhot = oneHotEncodeColumns(dfnumeric, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"))

// 7. merge data with VectorAssembler</br>
// 输入列是上面dfhot中的所有列、但除去"income"列。"income”列是预测目标，被重命名为“label”
import org.apache.spark.ml.feature.VectorAssembler

val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfhot.columns.diff(Array("income")))
val lpoints = va.transform(dfhot)
             .select("features", "income")
             .withColumnRenamed("income", "label")
~~~

### 8.2.3 Training the Model

1. LR模型原理

2. 与LR相关的Spark API
> LR can be trained in Spark by using `Logistic-RegressionWithSG`, `LogisticRegressionWithLBFGS` or new API `LogisticRegression`, which will give a `LogisticRegressionModel` object

3. 例子

~~~scala
// 拆分训练集、验证集
val splits = lpoints.randomSplit(Array(0.8, 0.2))
val adulttrain = splits(0).cache()
val adultvalid = splits(1).cache()

// 方法1： 模型初始化时指定超参数，并训练模型
import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression
lr.setRegParam(0.01)                  // L2正则化超参数
        .setMaxIter(1000)                // 最高迭代次数
        .setFitIntercept(true)        // 要求训练截距
val lrmodel = lr.fit(adulttrain)

// 方法2：在fit时通过ParamMap指定新的超参数
import org.apache.spark.ml.param.ParamMap
val lrmodel = lr.fit(adulttrain, ParamMap(
                                                                lr.regParam -> 0.01, 
                                                                lr.maxIter -> 500, 
                                                                lr.fitIntercept -> true))

// 打印LR模型的特征权重和截距
lrmodel.coefficients
lrmodel.intercept

// 使用模型对验证集做预测
// 
val validpredicts = lrmodel.transform(adultvalid)
validpredicts.show()
// probability:  [sample not in category, sample in category] 
// rawPrediction: [log-odds that sample not in category, log-odds that sample in category] 
// prediction: 0 or 1
// +--------------------+-----+-----------------+-----------------+----------+
// |            features|label|    rawPrediction|      probability|prediction|
// +--------------------+-----+-----------------+-----------------+----------+
// |(103,[0,1,2,4,5,6...|  0.0|[1.00751014104...|[0.73253259721...|       0.0|
// |(103,[0,1,2,4,5,6...|  0.0|[0.41118861448...|[0.60137285202...|       0.0|
// |(103,[0,1,2,4,5,6...|  0.0|[0.39603063020...|[0.59773360388...|       0.0|
// ...
// The names of all these columns (including features and label) can be customized using parameters (for example, outputCol, rawPredictionCol, probabilityCol, and so on).
~~~

### 8.2.4. Evaluating classification models

<b>Area under ROC, Area under PR</b>

use `BinaryClassificationEvaluator.evaluate()` function on the predict result (val `validpredicts`) in last section. 

~~~
// Area under ROC (default) 
val validpredicts = lrmodel.transform(adultvalid)
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val bceval = new BinaryClassificationEvaluator()
bceval.evaluate(validpredicts)
bceval.getMetricName
// res1: String = areaUnderROC

// Area under PR
bceval.setMetricName("areaUnderPR") // explicit set metric to PR
bceval.evaluate(validpredicts)
// res0: Double = 0.9039934862200736
~~~

<b>`TP`/`FP`/`TN`/`FN`/`P`(presion)/`R`(recall)/`TPR`/`F1`/`FPR`</b>

<b>PR Curve</b>

~~~scala
import org.apache.spark.ml.classification.LogisticRegressionModel
def computePRCurve(train:DataFrame, valid:DataFrame, lrmodel:LogisticRegressionModel) =
{
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    for(threshold <- 0 to 10)
    {
        var thr = threshold/10.0
        if(threshold == 10)
            thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.pr.collect()(1)
        println("%.1f: R=%f, P=%f".format(thr, pr._1, pr._2))
    }
}
computePRCurve(adulttrain, adultvalid, lrmodel)
// 0.0: R=1.000000, P=0.238081
// 0.1: R=0.963706, P=0.437827
// 0.2: R=0.891973, P=0.519135
// 0.3: R=0.794620, P=0.592486
// 0.4: R=0.694278, P=0.680905
// 0.5: R=0.578992, P=0.742200
// 0.6: R=0.457728, P=0.807837
// 0.7: R=0.324936, P=0.850279
// 0.8: R=0.202818, P=0.920543
// 0.9: R=0.084543, P=0.965854
// 1.0: R=0.019214, P=1.000000
~~~

<b>ROC Curve</b>

~~~scala
def computeROCCurve(train:DataFrame, valid:DataFrame, lrmodel:LogisticRegressionModel) =
{
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    for(threshold <- 0 to 10)
    {
        var thr = threshold/10.0
        if(threshold == 10)
            thr -= 0.001
        lrmodel.setThreshold(thr)
        val validpredicts = lrmodel.transform(valid)
        val validPredRdd = validpredicts.rdd.map(row => (row.getDouble(4), row.getDouble(1)))
        val bcm = new BinaryClassificationMetrics(validPredRdd)
        val pr = bcm.roc.collect()(1)
        println("%.1f: FPR=%f, TPR=%f".format(thr, pr._1, pr._2))
    }
}
computeROCCurve(adulttrain, adultvalid, lrmodel)
// 0,0: R=1,000000, P=0,237891
// 0,1: R=0,953708, P=0,430118
// 0,2: R=0,891556, P=0,515234
// 0,3: R=0,794256, P=0,586950
// 0,4: R=0,672525, P=0,668228
// 0,5: R=0,579511, P=0,735983
// 0,6: R=0,451350, P=0,783482
// 0,7: R=0,330047, P=0,861298
// 0,8: R=0,205315, P=0,926499
// 0,9: R=0,105444, P=0,972332
// 1,0: R=0,027004, P=1,000000
~~~

### 8.2.5 Cross Validate

~~~scala
// 初始化CrossValidator
import org.apache.spark.ml.tuning.CrossValidator
val cv = new CrossValidator()
                .setEstimator(lr)                        // model
                .setEvaluator(bceval)                // evaluator
                .setNumFolds(5)                                //fold

// 设置Parmeter Grid
import org.apache.spark.ml.tuning.ParamGridBuilder
val paramGrid = new ParamGridBuilder()
        .addGrid(lr.maxIter, Array(1000))
        .addGrid(lr.regParam, Array(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5))
        .build()
cv.setEstimatorParamMaps(paramGrid)

// 用CrossValidator训练模型
val cvmodel = cv.fit(adulttrain)

// 最佳模型特征权重
cvmodel
        .bestModel
        .asInstanceOf[LogisticRegressionModel]
        .coefficients
// res0: org.apache.spark.mllib.linalg.Vector = [0.0248435418248564,7.555156155398289E-7,3.1447428691767557E-4, 6.176181173588984E-4,0.027906992593851074,-0.7258527114344593,...

// 最佳模型的L2_REG超参数
cvmodel
        .bestModel
        .parent
        .asInstanceOf[LogisticRegression]
        .getRegParam
// res1: Double = 1.0E-4

// 用验证集数据评估模型效果（默认Area of ROC，可以选择不同评估标准）
new BinaryClassificationEvaluator()
        .evaluate(cvmodel.bestModel.transform(adultvalid))
~~~

### 8.2.6. Multiclass logistic regression

Approach 1: use `LogisticRegressionWithLBFGS`

> Spark ML logistic regression doesn not support multiclass classification at this time, but we can use MLlib`LogisticRegressionWithLBFGS`, just there is no space to introduce this 

Approach 2: `softmax`</br>

> [http://spark.apache.org/docs/2.1.1/ml-classification-regression.html](http://spark.apache.org/docs/2.1.1/ml-classification-regression.html)

Approach 3: use `OneVsRest`, `OneVsRestModel`, `MulticlassMetrics` 

> below is the example, include: </br>
> 1. data set</br>
> 2. load data set</br>
> 3. features and labels</br>
> 4. split train set and validate set</br>
> 5. build oneVsRest model</br>
> 6. train model</br>
> 7. predict on validate set</br>
> 8. evaluate the predict result</br>

~~~scala
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}

//1. Dataset
//数据集是penbased.dat
val penschema = StructType(Array(
    StructField("pix1",IntegerType,true),
    StructField("pix2",IntegerType,true),
    StructField("pix3",IntegerType,true),
    StructField("pix4",IntegerType,true),
    StructField("pix5",IntegerType,true),
    StructField("pix6",IntegerType,true),
    StructField("pix7",IntegerType,true),
    StructField("pix8",IntegerType,true),
    StructField("pix9",IntegerType,true),
    StructField("pix10",IntegerType,true),
    StructField("pix11",IntegerType,true),
    StructField("pix12",IntegerType,true),
    StructField("pix13",IntegerType,true),
    StructField("pix14",IntegerType,true),
    StructField("pix15",IntegerType,true),
    StructField("pix16",IntegerType,true),
    StructField("label",IntegerType,true)
))
val pen_raw = sc.textFile("first-edition/ch08/penbased.dat", 4)
    .map(x => x.split(", "))
    .map(row => row.map(x => x.toDouble.toInt))

// 2. load data set
import org.apache.spark.sql.Row
val dfpen = spark.createDataFrame(pen_raw.map(Row.fromSeq(_)), penschema)

// 3. features and labels
import org.apache.spark.ml.feature.VectorAssembler
val va = new VectorAssembler().setOutputCol("features")
va.setInputCols(dfpen.columns.diff(Array("label")))
val penlpoints = va.transform(dfpen).select("features", "label")

// 4. split train set and validate set
val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
val pentrain = pensets(0).cache()
val penvalid = pensets(1).cache()

// 5. build oneVsRest model
import org.apache.spark.ml.classification.OneVsRest
val penlr  = new LogisticRegression()   // LR
                     .setRegParam(0.01) // L2 Norm 0.01
val ovrest = new OneVsRest()            // OneVsRest 
ovrest.setClassifier(penlr)             // set LR into OneVsRest

// 6. train model
val ovrestmodel = ovrest.fit(pentrain)

// 7. predict on validate set
val penresult = ovrestmodel.transform(penvalid)
val penPreds  = penresult
                  .select("prediction", "label")
                  .rdd
                  .map(row => (row.getDouble(0), row.getDouble(1)))

// 8. evaluate the predict result
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val penmm = new MulticlassMetrics(penPreds)
penmm.precision      //0.9018214127054642
penmm.precision(3)   //0.9026548672566371
penmm.recall(3)      //0.9855072463768116
penmm.fMeasure(3)    //0.9422632794457274
penmm.confusionMatrix
// 228.0  1.0    0.0    0.0    1.0    0.0    1.0    0.0    10.0   1.0
// 0.0    167.0  27.0   3.0    0.0    19.0   0.0    0.0    0.0    0.0
// 0.0    11.0   217.0  0.0    0.0    0.0    0.0    2.0    0.0    0.0
// 0.0    0.0    0.0    204.0  1.0    0.0    0.0    1.0    0.0    1.0
// 0.0    0.0    1.0    0.0    231.0  1.0    2.0    0.0    0.0    2.0
// 0.0    0.0    1.0    9.0    0.0    153.0  9.0    0.0    9.0    34.0
// 0.0    0.0    0.0    0.0    1.0    0.0    213.0  0.0    2.0    0.0
// 0.0    14.0   2.0    6.0    3.0    1.0    0.0    199.0  1.0    0.0
// 7.0    7.0    0.0    1.0    0.0    4.0    0.0    1.0    195.0  0.0
// 1.0    9.0    0.0    3.0    3.0    7.0    0.0    1.0    0.0    223.0
~~~

## 8.3. Decision trees and random forests

<b>Decision trees</b>: </br>

> don’t require data normalization, they can handle numeric and categorical data, and they can work with missing value</br>
> prone to overfitting (covered in chapter 7), though, and are very sensitive to the input data</br>

<b>Random forest</b>: </br>

### 8.3.1 Decision trees

Inroduction of Decision Trees

> refer to the chapter of the book

parameter of `DecisionTreeClassifier` and `DecisionTreeRegressor`:</br>

> `maxDepth`: max tree depth</br>
> `maxBins` (default 32): max bin number for binning continuous feature</br>
> `minInstancesPerNode`</br>
> `minInfoGain`</br>
> ...


Example is as below, include 

> 1. category encoding for label column with StringIndexer</br>
> 2. split train-set and validate-set</br>
> 3. init Decision tree</br>
> 4. model training</br>
> 5. examing the root node</br>
> 6. check feature index of an internal node</br>
> 7. check the split threshold of an internal node</br>
> 8. examing the categories of an internal node</br>
> 9. evaluate the model with confusionMatrix for multi-classification problem</br>


~~~scala
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel

//1.label列category编码
val dtsi = new StringIndexer()
                    .setInputCol("label")       //输入列列名
                    .setOutputCol("label-ind")  //输出列列名
val dtsm:StringIndexerModel = dtsi.fit(penlpoints)
val pendtlpoints = dtsm
                    .transform(penlpoints)
                    .drop("label")
                    .withColumnRenamed("label-ind", "label")

//2.split train-set and validate-set
val pendtsets = pendtlpoints.randomSplit(Array(0.8, 0.2))
val pendttrain = pendtsets(0).cache()
val pendtvalid = pendtsets(1).cache()

//3.init Decision tree
import org.apache.spark.ml.classification.DecisionTreeClassifier
val dt = new DecisionTreeClassifier()
dt.setMaxDepth(20)    //超参数：最大深度

//4. model training 
val dtmodel = dt.fit(pendttrain)

//5. examing the root node
dtmodel.rootNode // impurity = 0.4296875, split = org.apache.spark.ml.tree.CategoricalSplit@557cc88b)

//6. check feature index of an internal node
import org.apache.spark.ml.tree.InternalNode
dtmodel.rootNode
       .asInstanceOf[InternalNode]
       .split.featureIndex  //res1: Int = 15

//7. check the split threshold of an internal node
import org.apache.spark.ml.tree.ContinuousSplit
dtmodel.rootNode.asInstanceOf[InternalNode].split.asInstanceOf[ContinuousSplit].threshold          // 51

//8. examing the categories of an internal node 
//如果该节点根据一个类别特征进行分类，其类型将是一个CategoricalSplit，其属性leftCategories、rightCategories存储了具体的类型
dtmodel.rootNode.asInstanceOf[InternalNode].leftChild
dtmodel.rootNode.asInstanceOf[InternalNode].rightChild

//9. evaluate the model with confusionMatrix for multi-classification problem
//本例是一个多分类问题、使用MulticlassMetrics
val dtpredicts = dtmodel.transform(pendtvalid)
val dtresrdd   = dtpredicts
                .select("prediction", "label")
                .rdd
                .map(row => (row.getDouble(0), row.getDouble(1)))
val dtmm = new MulticlassMetrics(dtresrdd)
dtmm.precision  //0.951442968392121
dtmm.confusionMatrix
// 192.0  0.0    0.0    9.0    2.0    0.0    2.0    0.0    0.0    0.0
// 0.0    225.0  0.0    1.0    0.0    1.0    0.0    0.0    3.0    2.0
// 0.0    1.0    217.0  1.0    0.0    1.0    0.0    1.0    1.0    0.0
// 9.0    1.0    0.0    205.0  5.0    1.0    3.0    1.0    1.0    0.0
// 2.0    0.0    1.0    1.0    221.0  0.0    2.0    3.0    0.0    0.0
// 0.0    1.0    0.0    1.0    0.0    201.0  0.0    0.0    0.0    1.0
// 2.0    1.0    0.0    2.0    1.0    0.0    207.0  0.0    2.0    3.0
// 0.0    0.0    3.0    1.0    1.0    0.0    1.0    213.0  1.0    2.0
// 0.0    0.0    0.0    2.0    0.0    2.0    2.0    4.0    198.0  6.0
// 0.0    1.0    0.0    0.0    1.0    0.0    3.0    3.0    4.0    198.0
~~~

### 8.3.2. Random forests

Inroduction of Rondom Forests

> refer to the chapter of the book

Random Forests in Spark

> class: `RandomForestClassifier`, `RandomForestRegressor`</br>
> parameters: </br>
> (1) parameters of Decision Trees</br>
> (2) parameters about the forest: `numTrees`,`featureSubsetStrategy` (`all`,`onethird`,`sqrt`,`log2`,`auto`(sqrt for classification, onethird for regression), ...

Example: 

~~~scala
import org.apache.spark.ml.classification.RandomForestClassifier
// 初始化
val rf = new RandomForestClassifier()
// 设置超参数
rf.setMaxDepth(20)
// 训练模型
val rfmodel = rf.fit(pendttrain)
// 打印Rondom Forest的结构信息
rfmodel.trees
// res0: Array[org.apache.spark.ml.tree.DecisionTreeModel] =
// Array(DecisionTreeClassificationModel of depth 20 with 833 nodes,
// DecisionTreeClassificationModel of depth 17 with 757 nodes,
// DecisionTreeClassificationModel of depth 16 with 691 nodes, ...
// 用验证集验证
val rfpredicts = rfmodel.transform(pendtvalid)
// 评估验证结果
val rfresrdd = rfpredicts.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val rfmm = new MulticlassMetrics(rfresrdd)
rfmm.precision
//0.9894640403114979
rfmm.confusionMatrix
// 205.0  0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0
// 0.0    231.0  0.0    0.0    0.0    0.0    0.0    0.0    1.0    0.0
// 0.0    0.0    221.0  1.0    0.0    0.0    0.0    0.0    0.0    0.0
// 5.0    0.0    0.0    219.0  0.0    0.0    2.0    0.0    0.0    0.0
// 0.0    0.0    0.0    0.0    230.0  0.0    0.0    0.0    0.0    0.0
// 0.0    1.0    0.0    0.0    0.0    203.0  0.0    0.0    0.0    0.0
// 1.0    0.0    0.0    1.0    0.0    0.0    216.0  0.0    0.0    0.0
// 0.0    0.0    1.0    0.0    2.0    0.0    0.0    219.0  0.0    0.0
// 0.0    0.0    0.0    1.0    0.0    0.0    0.0    1.0    212.0  0.0
// 0.0    0.0    0.0    0.0    0.0    0.0    2.0    2.0    2.0    204.0
~~~

## 8.4. Using k-means clustering

Clustering in Spark: 

> K-means clustering</br>
> Gaussian mixture model</br>
> Power-iteration clustering</br>

Kmeans Clustering: 假定模型是相同方差的正态分布

> trouble handling non-spherical clusters and unevenly sized clusters (uneven by density or by radius)</br>
> can’t make efficient use of the one-hot-encoded features</br>
> often used for classifying text documents, along with the term frequency-inverse document frequency (TF-IDF) feature-vectorization method</br>

Gaussian mixture model: 

> the model is a mixture of a Gaussian distributions</br>
> modeling a probability that an example belongs to a cluster</br>
> doesn’t scale well to datasets with many dimensions</br>

Power-iteration clustering:

> is a form of spectral clustering
> based on the GraphX library and will be introduced in next Chapter

Use `elbow method` to determine the number of clusters: 

> 1. gradually increase the number of clusters</br>
> 2. look at the cost of each model (cost will inevitably decrease) as the clusters-number increases</br>
> 3. plot the cost as a function of number of clusters K</br>
> 4. will notice a few “elbows” where the slope of the function changes abruptly (the corresponding K is the optimal cluster number)

### 8.4.1. K-means clustering

Introduction to the algorithms: 

> [https://livebook.manning.com/book/spark-in-action/chapter-8/297](https://livebook.manning.com/book/spark-in-action/chapter-8/297)

Using k-means clustering in Spark: 

Parameters of `KMeans`: 

> `k`: Number of clusters to find (default is 2)<br/> 
> `maxIter`: Maximum number of iterations to perform<br/>
> `predictionCol`: Prediction column name (default is “prediction”)</br>
> `featuresCol`: Features column name (default is “features”)</br>
> `tol`: Convergence tolerance</br>
> `seed`: random seed</br>

Example is as below, include </br>

> 1. Model Training </br>
> 2. Compute model `cost` (also called `distortion`), calculated as the `sum` of the `squared distances` from `all points` to the `matching cluster centers`</br>
> 3. predict cluster_id with validate-set</br>
> 4. Plot with Contingency tabel when examples are labeled</br>

~~~scala
// 1. Model Training
import org.apache.spark.ml.clustering.KMeans
//  初始化Kmeans模型
val kmeans = new KMeans()
//  模型超参数
kmeans.setK(10)
kmeans.setMaxIter(500)
//  模型训练
val kmmodel = kmeans.fit(penlpoints)

// 2. computing model cost
//   样本到所属聚类中心距离平方之和
//   4.517530920539787E7
kmmodel.computeCost(penlpoints) 
//   除以样本数得到的数值会更加直观
//   67.5102817068467
math.sqrt(kmmodel.computeCost(penlpoints)/penlpoints.count()) 

// 3. predict cluster_id with validate-set
val kmpredicts = kmmodel.transform(penlpoints)

// 4. Plot with Contingency tabel
// 样本有标签时，可以使用列联表（contingency tabel）来分析聚类效果
//  聚类后每个样本都有cluster_id, label
//  (1) 如何把cluster_id与label对应起来： 把每个label与其样本量最多的cluster_id对应起来
//      有可能会出现7和2对应错，8和0对应错这样的情况
//  (2) 列联表元素内容(i行j列)：符合label i与cluster j样本的数量
//  (3) 纯净度（Purity）：正确聚类的比例
import org.apache.spark.rdd.RDD
def printContingency(
    // df: 包含两列Label和Predict Value
    df:org.apache.spark.sql.DataFrame, 
    // labels: 所有可能的标签值
    labels:Seq[Int])
{
    // 
    val rdd:RDD[(Int, Int)] = df
           .select('label, 'prediction)
           .rdd.map(row => (row.getInt(0), row.getInt(1)))
           .cache()
    val numl = labels.size
    val tablew = 6*numl + 10
    var divider = "".padTo(10, '-')
    for(l <- labels) {
        divider += "+-----"
    }

    var sum:Long = 0
    print("orig.class")
    for(l <- labels) {
        print("|Pred"+l)
    }
    println
    println(divider)
    val labelMap = scala.collection.mutable.Map[Int, (Int, Long)]()
    for(l <- labels) {
        //filtering by predicted labels
        val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
        //get the cluster with most elements
        val topLabelCount = predCounts.sortBy{-_._2}.apply(0)
        //if there are two (or more) clusters for the same label
        if(labelMap.contains(topLabelCount._1)) {
            //and the other cluster has fewer elements, replace it
            if(labelMap(topLabelCount._1)._2 < topLabelCount._2) {
                sum -= labelMap(l)._2
                labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
                sum += topLabelCount._2
            }
            //else leave the previous cluster in
        } else {
            labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
            sum += topLabelCount._2
        }
        val predictions = predCounts.sortBy{_._1}.iterator
        var predcount = predictions.next()
        print("%6d".format(l)+"    ")
        for(predl <- labels) {
            if(predcount._1 == predl) {
                print("|%5d".format(predcount._2))
                if(predictions.hasNext) {
                    predcount = predictions.next()
                }
            } else {
                print("|    0")
            }
        }
        println
        println(divider)
    }
    rdd.unpersist()
    println("Purity: "+sum.toDouble/rdd.count())
    println("Predicted->original label map: "+labelMap.mapValues(x => x._1))
}
printContingency(kmpredicts, 0 to 9)
// orig.class|Pred0|Pred1|Pred2|Pred3|Pred4|Pred5|Pred6|Pred7|Pred8|Pred9
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      0    |    1|  379|   14|    7|    2|  713|    0|    0|   25|    2
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      1    |  333|    0|    9|    1|  642|    0|   88|    0|    0|   70
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      2    | 1130|    0|    0|    0|   14|    0|    0|    0|    0|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      3    |    1|    0|    0|    1|   24|    0| 1027|    0|    0|    2
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      4    |    1|    0|   51| 1046|   13|    0|    1|    0|    0|   32
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      5    |    0|    0|    6|    0|    0|    0|  235|  624|    3|  187
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      6    |    0|    0| 1052|    3|    0|    0|    0|    1|    0|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      7    |  903|    0|    1|    1|  154|    0|   78|    4|    1|    0
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      8    |   32|  433|    6|    0|    0|   16|  106|   22|  436|    4
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//      9    |    9|    0|    1|   88|   82|   11|  199|    0|    1|  664
// ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
// Purity: 0.6672125181950509
// Predicted->original label map: Map(8 -> 8, 2 -> 6, 5 -> 0, 4 -> 1, 7 -> 5, 9 -> 9, 3 -> 4, 6 -> 3, 0 -> 2)
~~~

