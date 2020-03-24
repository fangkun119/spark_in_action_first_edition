# CH07 Getting smart with MLlib

Table of Contents: </br>

> 7.1. Introduction to machine learning </br>
> 7.2. Linear algebra in Spark </br>
> 7.3. Linear regression </br>
> 7.4. Analyzing and preparing the data </br>
> 7.5. Fitting and using a linear regression model </br>
> 7.6. Tweaking the algorithm </br>
> 7.7. Optimizing linear regression </br>
> 7.8. Summary </br>

Documents: </br>

> [http://spark.apache.org/docs/latest/ml-guide.html](http://spark.apache.org/docs/latest/ml-guide.html)</br>
> [http://spark.apache.org/examples.html](http://spark.apache.org/examples.html)
> [https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples)

## 7.1 Introduction to Machine Learning

> Frameworks:  `Spark`, `GraphLab`, `Flink`, `TensorFlow`, ...

Typical Steps: 

> `Collect Data: SparkStreaming`, `Cleasing and Preparing Data`, 
`Analysing Data and Extract Features`, `Training Model`, `Evaluating Model`, `Using Model`

### 7.1.1. Definition of machine learning

### 7.1.2. Classification of machine-learning algorithms

### 7.1.3. Machine learning with Spark

Low level API for speed up optimized linear algebra operations:

> Breeze for scala; jblas for java; NumPy for Python </br>
> doc: [http://mng.bz/417O](http://mng.bz/417O)

## 7.2. Linear algebra in Spark

> Linear Algrebra background:  [appendix C](https://livebook.manning.com/book/spark-in-action/appendix-c/app03) </br>
> `Sparse Vector/Matrix` and `Dense Vector/Matrix`</br>

### 7.2.1. Local vector and matrix implementations

package: `org.apache.spark.mllib.linalg` </br>

<b>start the shell in local mode: `spark-shell --master local[*]` (or just use the spark-in-action VM, then create the vectors</b>

~~~scala
import org.apache.spark.mllib.linalg.{Vectors,Vector}

// create dense vectors
val dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
val dv2 = Vectors.dense(Array(5.0,6.0,7.0,8.0))
// access there values
dv2(2)       // res0: Double = 7.0
dv1.size     // res1: Int = 4
dv2.toArray  // res2: Array[Double] = Array(5.0, 6.0, 7.0, 8.0)

// sparse vector: size, indice array, value array
val sv = Vectors.sparse(4, Array(0,1,2,3), Array(5.0,6.0,7.0,8.0))
~~~

<b>creating dense matrix</b>

~~~scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix,
Matrix, Matrices}
import breeze.linalg.{DenseMatrix => BDM,CSCMatrix => BSM,Matrix => BM}
val dm = Matrices.dense(2,3,Array(5.0,0.0,0.0,3.0,1.0,4.0))
// parameters: row, column, data
// output: dm: org.apache.spark.mllib.linalg.Matrix = 
// 5.0  0.0  1.0
// 0.0  3.0  4.0
~~~

<b>matrix generating functions</b>

sample code: [https://github.com/kongcong/SparkMllibDemo/blob/master/src/com/xj/ml/vectormatrix/MatrixCompanionObjectDemo.scala](https://github.com/kongcong/SparkMllibDemo/blob/master/src/com/xj/ml/vectormatrix/MatrixCompanionObjectDemo.scala)

~~~scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix,
Matrix, Matrices}
import scala.util.Random

// DenseMatrix with 2 rows 3 columns of Uniform Distribution
val dmUniform: Matrix = Matrices.rand(2, 3, Random.self);

// DenseMatrix with 2 rows 3 columns of Gaussian Distribution
val dmGaussian: Matrix = Matrices.randn(2, 3, Random.self);

// SparseMatrix with 2 rows 3 columns of Uniform Distribution and density = 0.2
val smUniform: Matrix = Matrices.sprand(2, 3, 0.2, Random.self);

// SparseMatrix with 2 rows 3 columns of Gaussian Distribution and density = 0.2
val smGaussian: Matrix = Matrices.sprandn(2, 3, 0.2, Random.self);

// DenseMatrix with 2 rows 3 columsn of element 1
val ones: Matrix = Matrices.ones(2, 3);

// matrix with 2 rows 3 columsn of element 1
val zeros: Matrix = Matrices.zeros(2, 3)

// dense identity matrix with 4 elements
val eye: Matrix = Matrices.eye(4);

// sparse identity matrix with 4 elements
val speye: _root_.org.apache.spark.mllib.linalg.SparseMatrix = SparseMatrix.speye(4)

// diagnal matrix
val diag: Matrix = Matrices.diag(Vectors.dense(1, 2, 3, 4))

// concat 2 matrix horizenly
val m1 = Matrices.dense(3, 3, Array(1, 2, 3, 4, 5, 6, 7, 8, 9))
val m2 = Matrices.dense(3, 3, Array(1, 1, 1, 1, 1, 1, 1, 1, 1))
val horzcat: Matrix = Matrices.horzcat(Array(m1, m2))
println(horzcat)
/**
 * 1.0  4.0  7.0  1.0  1.0  1.0
 * 2.0  5.0  8.0  1.0  1.0  1.0
 * 3.0  6.0  9.0  1.0  1.0  1.0
 */

// concat 2 matrix vertically
val vertcat: Matrix = Matrices.vertcat(Array(m1, m2))
println(vertcat)
/**
 * 1.0  4.0  7.0
 * 2.0  5.0  8.0
 * 3.0  6.0  9.0
 * 1.0  1.0  1.0
 * 1.0  1.0  1.0
 * 1.0  1.0  1.0
 */
~~~

> These methods (`eye`, `rand`, `randn`, `zeros`, `ones`, and `diag`) aren’t available in Python

<b>creating local sparse matrix</b>

~~~scala
// 2 rows, 3 columns, 
// colPtrs    = [0 1 2 4], 
// rowIndices = [0 1 0 1], 
// elements   = [5 3 1 4], 
val sm = Matrices.sparse(2,3,Array(0,1,2,4), Array(0,1,0,1), Array(5.,3.,1.,4.))
~~~

`SparseMatrix` isn’t available in Python.

<b>convert sparse matrix and dense matrix</b>

~~~scala
import org.apache.spark.mllib.linalg. {DenseMatrix,SparseMatrix}
sm.asInstanceOf[SparseMatrix].toDense
// res0: org.apache.spark.mllib.linalg.DenseMatrix =
// 5.0  0.0  1.0
// 0.0  3.0  4.0
dm.asInstanceOf[DenseMatrix].toSparse
// 2 x 3 CSCMatrix
// (0,0) 5.0
// (1,1) 3.0
// (0,2) 1.0
// (1,2) 4.0
~~~

<b>access element in matrix by index</b>

~~~scala
dm(1,1)
// res1: Double = 3.0
~~~

<b>trnaspose</b>

~~~scala
dm.transpose
// res1: org.apache.spark.mllib.linalg.Matrix =
// 5.0  0.0
// 0.0  3.0
// 1.0  4.0
~~~

<b>convert local `Spark Vector` to `Breeze Class`</b>

> `Breeze` is the underline linear algebra liberary. Spark makes them private thus they won't exposed to the user. If need this 3rd lib, below is the approach to convert `Spark Vector` to `Breeze Class`

~~~scala
// rename the class to prevent naming confliction with Spark!
import breeze.linalg.{DenseVector => BDV,SparseVector => BSV,Vector => BV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}

def toBreezeV(v:Vector):BV[Double] = v match {
    case dv:DenseVector  => new BDV(dv.values)
    case sv:SparseVector => new BSV(sv.indices, sv.values, sv.size)
}

// Breeze provide more operations than Spark Vector
toBreezeV(dv1) + toBreezeV(dv2)    // res3: breeze.linalg.Vector[Double] = DenseVector(10.0, 12.0, 14.0, 16.0)
toBreezeV(dv1).dot(toBreezeV(dv2)) // res4: Double = 174.0
~~~

<b>convert local `Spark Matrix` to `Breeze Class`</b>

> similar as for vector as above

~~~scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix, Matrices}
import breeze.linalg.{DenseMatrix => BDM,CSCMatrix => BSM,Matrix => BM}
def toBreezeM(m:Matrix):BM[Double] = m match {
    case dm:DenseMatrix => new BDM(dm.numRows, dm.numCols, dm.values)
    case sm:SparseMatrix => new BSM(sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
}
~~~

### 7.2.2 Distributed Matrices 

Package: `org.apache.spark.mllib.linalg.distributed` <br/>
Index: using `Long` instead of `Int` <br/>
Four types of Distributed Matrices: `RowMatrix`, `IndexedRowMatrix`, `BlockMatrix`, `CoordinateMatrix` <br/>

#### `RowMatrix`

`RowMatrix`: store rows in `RDD Vector` objects</br>
`rows()`: get the rows</br>
`numRows(), numCols()`: get row number and column number</br>

Notice that every kinds of distributed Matrix can be converted to `RowMatrix` by `toRowMatrix()` method, but can not convert back


#### `IndexedRowMatrix`

~~~scala
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
val rmind = new IndexedRowMatrix(rm.rows.zipWithIndex().map(x => IndexedRow(x._2, x._1)))
~~~

RDD of `IndexedRow` objects, each contains an `index of row` and a `Vector` to store the raw data

#### `CoordinateMatrix`

Store values in `MatrixEntry` objects which contains `(i,j)` positions.

It consumes too much memory, thus only suitable for sparse matrix

#### `BlockMatrix`

Stores values as RDDs of tuples `((i,j), Matrix)`

> It contains `local matrices (blocks)` referenced by their position in the matrix. These `blocks` have the same sizes except those `last blocks`.<br/> 
> The `validate` method checks whether all blocks are of the same size (except the last ones)

Is the only distributed implementation with methods for `adding` and `multiplying` other distributed matrices.

#### Linear algebra operations with distributed matrices

They are limited in the lib, need to implement some of them by ourself, for example: 

* `Element-wise addition and multiplication` are only available for `BlockMatrix` 
* `Transposition` is available only for `CoordinateMatrix` and `BlockMatrix`.
* No implementation for `matrix inverse`

#### Convert `distributed Matrix` to `Breeze class` 

~~~scala
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
def toBreezeD(dm:DistributedMatrix):BM[Double] = dm match {
    case rm:RowMatrix => {
      val m = rm.numRows().toInt
       val n = rm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       var i = 0
       rm.rows.collect().foreach { vector =>
         for(j <- 0 to vector.size-1)
         {
           mat(i, j) = vector(j)
         }
         i += 1
       }
       mat
     }
    case cm:CoordinateMatrix => {
       val m = cm.numRows().toInt
       val n = cm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       cm.entries.collect().foreach { case MatrixEntry(i, j, value) =>
         mat(i.toInt, j.toInt) = value
       }
       mat
    }
    case bm:BlockMatrix => {
       val localMat = bm.toLocalMatrix()
       new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
    }
}
~~~

## 7.3. Linear regression

### 7.3.1. About linear regression

### 7.3.2. Simple linear regression

Dataset: [Housing Data Set, UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/Housing)

> use "h(x) = w<sub>0</sub> + w<sub>1</sub>x" to explain Linear Regression </br>
> x: number of rooms </br>
> h: houser price


### 7.3.3. Multiple linear regression

> use "h(x) = w<sub>0</sub> + w<sub>1</sub>x<sub>1</sub> + ... + w<sub>n</sub>x<sub>n</sub> = w<sup>T</sup>x" to explain linear regression

## 7.4. Analyzing and preparing the data

> download dataset from [https://github.com/spark-in-action/first-edition/blob/master/ch07/housing.data](https://github.com/spark-in-action/first-edition/blob/master/ch07/housing.data) so that we can change the dataset as our need

<b>Analyzing the data</b>

> belows is the code, the topic includes: </br>
> 1. load the dataset </br>
> 2. column summary statistics with `RowMatrix` </br>
> 3. column summary statistics with `mllib.stat.Statistics` </br>
> 4. feature correlations with cosine distances </br>
> 5. feature correlations with covariance matrix</br>
> 6. feature correlations with Spearman’s and Pearson’s methods</br>

~~~python
import org.apache.spark.mllib.linalg.Vectors

# 1.load the dataset 
# 数据：每行一个样本，没列一个浮点数代表一个特征值，用逗号分隔
#  载入：分6个分区存储，housingVals每个原始是一个vector<double>，代表一个样本
val housingLines
    = sc.textFile("first-edition/ch07/housing.data", 6) 
val housingVals = housingLines.map(
    x => Vectors.dense(x.split(",").map(_.trim().toDouble)))

# 2. column summary statistics with `RowMatrix` 
# 分析数据分布：使用前面介绍的RowMatrix
val housingMat = new RowMatrix(housingVals)
val housingStats = housingMat.computeColumnSummaryStatistics()
housingStats.min

# 3. column summary statistics with `mllib.stat.Statistics` 
# 分析数据分布：也可以使用Statistics类
import org.apache.spark.mllib.stat.Statistics
val housingStats = Statistics.colStats(housingVals)
housingStats.min # res0: org.apache.spark.mllib.linalg.Vector = [0.00632,0.0,0.46,0.0,0.385,3.561,2.9,1.1296,1.0,187.0,12.6,0.32,1.73,5.0]
housingStats.max
housingStats.mean
housingStats.normL1 # sum of absolute values of all elements per column
housingStats.normL2 # also called Euclidian norm; equal to the length of a vector/column
housingStats.variance

# 4. feature similarity with cosine distances
# 分析各个列之间的预先相似度 （该方法在Python中不可用），返回一个上三角矩阵，矩阵元素表示两个特征的离弦相似度（第i行j列元素，表示特征i与特征j的离弦相似度，取值范围[-1,+1]，-1表示完全相反、1表示两个特征相同）
val housingColSims = housingMat.columnSimilarities()
# 查看相似度的值
def printMat(mat:BM[Double]) = {
   print("            ")
   for(j <- 0 to mat.cols-1) print("%-10d".format(j));
   println
   for(i <- 0 to mat.rows-1) { print("%-6d".format(i)); for(j <- 0 to mat.cols-1) print(" %+9.3f".format(mat(i, j))); println }
}
printMat(toBreezeD(housingColSims)) #toBreezeD is defined in previouse code segments

# 5. compute the feature correlations with covariance matrix
# 返回一个对称矩阵，每个元素对应两个特征（横坐标、纵坐标），值为0表示线性不相关，值大于0表示线性正相关，值小于0表示线性负相关
val housingCovar = housingMat.computeCovariance()
printMat(toBreezeM(housingCovar))

# 6. compute the feature correlations with Spearman’s and Pearson’s methods 
# use method provided by org.apache.spark.mllib .stat.Statistics
~~~

<b>Preparing the data</b>

> follow up the code above, prepare the training and testing data set with code below, includes: </br>
> 1. transform to labeled points</br>
> 2. split training and validating set</br>
> 3. feature scaling and mean normalization</br>

~~~scala
import org.apache.spark.mllib.regression.LabeledPoint

# 1. transforming to labeled points
# housingVals每个元素的一个vector<double>代表一个样本
# housingData每个原始是一个LabeledPoint，参数1是label，参数2是特征
val housingData = housingVals.map(x => {
  val a = x.toArray
  LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1)))
})

# 2. split training and validating set
val sets = housingData.randomSplit(Array(0.8, 0.2))
val housingTrain = sets(0)
val housingValid = sets(1)

# 3. feature scaling and mean normalization
import org.apache.spark.mllib.feature.StandardScaler
val scaler = new StandardScaler(true, true)
                    .fit(housingTrain.map(x => x.features))
val trainScaled = housingTrain.map(x => LabeledPoint(x.label,
 scaler.transform(x.features)))
val validScaled = housingValid.map(x => LabeledPoint(x.label,
 scaler.transform(x.features)))
~~~

## 7.5 Fitting and using a linear regression model

> package: `org.apache.spark.mllib.regression`
> classes: `LinearRegressionModel`, `LinearRegressionWithSGD`

<b>Fitting the Model</b>

There are 2 ways to train the model: </br>
The 1st way (standard Spark way) is invoking the static `train` method as below, but `it can not find the intercept value (only the weights)`</br>

~~~scala
val model = LinearRegressionWithSGD.train(trainScaled, 200, 1.0)
~~~

The 2nd way can find the intercept as below

~~~scala
val alg = new LinearRegressionWithSGD() 
alg.setIntercept(true)    
// ...
alg.run(trainScaled)
~~~

below is the completed code to train the model, including: 

> 1. train with train-set</br>
> 2. predect with validate-set</br>
> 3. evaluate model on validate-set</br>
> 4. interpreting the model parameters</br>
> 5. save the model</br>

~~~scala
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// 1. training with train-set
val alg = new LinearRegressionWithSGD() // init the objects
alg.setIntercept(true)                  // set opt to find intercept
alg.optimizer.setNumIterations(200)     // iteration number
trainScaled.cache()                     // caching is importent
validScaled.cache()
val model = alg.run(trainScaled)        // start to train model

// 2. predect with validate-set
val validPredicts = validScaled.map(x => (model.predict(x.features), x.label))
validPredicts.collect()
// res123: Array[(Double, Double)] = Array((28.250971806168213,33.4),(23.050776311791807,22.9), (21.278600156174313,21.7), (19.067817892581136,19.9), (19.463816495227626,18.4), ...
val RMSE = math.sqrt(validPredicts.map{case(p,l) => math.pow(p-l,2)}.mean())
// res0: Double = 4.775608317676729

// 3. evaluate model on validate-set
import org.apache.spark.mllib.evaluation.RegressionMetrics
val validMetrics = new RegressionMetrics(validPredicts)
validMetrics.rootMeanSquaredError
// res1: Double = 4.775608317676729
validMetrics.meanSquaredError
// res2: Double = 22.806434803863162
validMetrics.meanAbsoluteError # Average absolute difference
validMetrics.r2                # 
validMetrics.explainedVariance # A value similar to R2 

// 4. interpreting the model parameters
// If a particular weight is near zero, the corresponding dimension doesn’t contribute to the target variable (price of housing) in a significant way (assuming the data has been scaled—otherwise even low-range features might be important)
println(
    model.weights
        .toArray             # convert to Spark Array
        .map(x => x.abs)     # we only want to check the signaficance of features
        .zipWithIndex        # index is attached to each weight
        .sortBy(_._1)        # sort by abs(weight)
        .mkString(", ")      # convert to string
)
// (0.112892822124492423,6), (0.163296952677502576,2), (0.588838584855835963,3), (0.939646889835077461,0), (0.994950411719257694,11), (1.263479388579985779,1),(1.660835069779720992,9), (2.030167784111269705,4), (2.072353314616951604,10), (2.419153951711214781,8), (2.794657721841373189,5), (3.113566843160460237,7),(3.323924359136577734,12)

// 5. loading and saving the model
// model is saved as a Parquet file in hdfs
model.save(sc, "hdfs:///path/to/saved/model")
~~~

> `R2`: r2—Coefficient of determination R2 (0.71 in this case) is a value between 0 and 1 and represents the fraction of variance explained. It’s a measure of how much a model accounts for the variation in the target variable (predictions) and how much of it is “unexplained.” A value close to 1 means the model explains a large part of variance in the target variable.

<b>using the model</b>

load the model as below, then it can be used to predict other dataset 

~~~scala
import org.apache.spark.mllib.regression.LinearRegressionModel
val model = LinearRegressionModel.load(sc, "hdfs:///path/to/saved/model")
~~~

## 7.6 Tweaking the algorithm

### 7.6.1. Finding the right step size and number of iterations

~~~scala
import org.apache.spark.rdd.RDD

// 功能类似于GridSearch的函数，用来寻找最佳 iteration_number 及 step_size 超参数
def iterateLRwSGD(
            iterNums:Array[Int],     // 迭代次数超参数
            stepSizes:Array[Double], // 梯度下降步长超参数
            train:RDD[LabeledPoint], // 训练集
            test:RDD[LabeledPoint]   // 验证集
    ) = {
    for(numIter <- iterNums; step <- stepSizes) {
        // 初始化模型
        val alg = new LinearRegressionWithSGD()
        // 设置模型参数：
        alg.setIntercept(true)            // need to find intercept
            .optimizer                    
            .setNumIterations(numIter)    // number of iterations
            .setStepSize(step)            // step size
        // 训练模型
        val model = alg.run(train)
        // 用训练好的模型在训练集、验证集上做预测
        val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
        val validPredicts = test.map(x => (model.predict(x.features), x.label))
        // 训练集、验证集均方误差
        val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
        // 查看当前超参数组合下模型的特征权重及截距
        // Uncomment if you wish to see weghts and intercept values:
        // println("%d, %4.2f -> %.4f, %.4f (%s, %f)".format(numIter, step, meanSquared,meanSquaredValid, model.weights, model.intercept))
  }
}

// 使用上述代码进行超参数搜索
iterateLRwSGD(Array(200, 400, 600), Array(0.05, 0.1, 0.5, 1, 1.5, 2, 3), trainScaled, validScaled)
// Our results:
// 200, 0.050 -> 7.5420, 7.4786
// 200, 0.100 -> 5.0437, 5.0910
// 200, 0.500 -> 4.6920, 4.7814
// 200, 1.000 -> 4.6777, 4.7756
// 200, 1.500 -> 4.6751, 4.7761
// 200, 2.000 -> 4.6746, 4.7771
// 200, 3.000 -> 108738480856.3940, 122956877593.1419
// 400, 0.050 -> 5.8161, 5.8254
// 400, 0.100 -> 4.8069, 4.8689
// 400, 0.500 -> 4.6826, 4.7772
// 400, 1.000 -> 4.6753, 4.7760
// 400, 1.500 -> 4.6746, 4.7774
// 400, 2.000 -> 4.6745, 4.7780
// 400, 3.000 -> 25240554554.3096, 30621674955.1730
// 600, 0.050 -> 5.2510, 5.2877
// 600, 0.100 -> 4.7667, 4.8332
// 600, 0.500 -> 4.6792, 4.7759
// 600, 1.000 -> 4.6748, 4.7767
// 600, 1.500 -> 4.6745, 4.7779
// 600, 2.000 -> 4.6745, 4.7783
// 600, 3.000 -> 4977766834.6285, 6036973314.0450
~~~

### 7.6.2. Adding higher-order polynomials

> add `higher-order polynomials` features，for examplete：expend h(x) = w<sub>0</sub>x<sub>3</sub> + w<sub>1</sub>x<sub>2</sub> + w<sub>2</sub>x + w<sub>3</sub> by adding `higher-order polynomials` features </br> 
> 增加多项式特征，进而增加模型复杂度</br>
> code is as below, including:</br>
> 1. expand features with additional features containing second-order polynomials</br>
> 2. split the training-set and testing-set</br>
> 3. scale the features</br>
> 4. grad search the hyper-parameters</br>
> 5. over-fitting when iteration number is too large</br>

~~~scala
// 1. expand features additional features containing second-order polynomials 
// 给每个特征增加一个二次方的特征
def addHighPols(v:Vector): Vector = {
  Vectors.dense(v.toArray.flatMap(x => Array(x, x*x)))
}
val housingHP = housingData.map(v => LabeledPoint(v.label, addHighPols(v.features)))
// 现在已经有26个特征了
housingHP.first().features.size
// res0: Int = 26

// 2. split train-set and test-set
val setsHP = housingHP.randomSplit(Array(0.8, 0.2))
val housingHPTrain = setsHP(0)
val housingHPValid = setsHP(1)

// 3. scale the features
val scalerHP = new StandardScaler(true, true).fit(housingHPTrain.map(x => x.features))
val trainHPScaled = housingHPTrain.map(
        x => LabeledPoint(x.label, scalerHP.transform(x.features)))
val validHPScaled = housingHPValid.map(
        x => LabeledPoint(x.label, scalerHP.transform(x.features)))
trainHPScaled.cache()
validHPScaled.cache()

// 4. grad search the hyper-parameters
iterateLRwSGD(
    Array(200, 400),  // iteration numbers
    Array(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5),  // step sizes
    trainHPScaled, 
    validHPScaled)
// Our results:
// 200, 0.400 -> 4.5423, 4.2002
// 200, 0.500 -> 4.4632, 4.1532
// 200, 0.600 -> 4.3946, 4.1150
// 200, 0.700 -> 4.3349, 4.0841
// 200, 0.900 -> 4.2366, 4.0392
// 200, 1.000 -> 4.1961, 4.0233
// 200, 1.100 -> 4.1605, 4.0108
// 200, 1.200 -> 4.1843, 4.0157
// 200, 1.300 -> 165.8268, 186.6295
// 200, 1.500 -> 182020974.1549, 186781045.5643
// 400, 0.400 -> 4.4117, 4.1243
// 400, 0.500 -> 4.3254, 4.0795
// 400, 0.600 -> 4.2540, 4.0466
// 400, 0.700 -> 4.1947, 4.0228
// 400, 0.900 -> 4.1032, 3.9947
// 400, 1.000 -> 4.0678, 3.9876
// 400, 1.100 -> 4.0378, 3.9836      // lower testing-error than before
// 400, 1.200 -> 4.0407, 3.9863
// 400, 1.300 -> 106.0047, 121.4576
// 400, 1.500 -> 162153976.4283, 163000519.6179

// 5. over-fitting when iteration number is too large
// 如果增加iteration number，testing-error反而会增高，发生了过拟合
iterateLRwSGD(
    Array(200, 400, 800, 1000, 3000, 6000), // iteration numbers
    Array(1.1),                             // step sizes
    trainHPScaled, 
    validHPScaled)
//Our results:
// 200, 1.100 -> 4.1605, 4.0108
// 400, 1.100 -> 4.0378, 3.9836
// 800, 1.100 -> 3.9438, 3.9901
// 1000, 1.100 -> 3.9199, 3.9982
// 3000, 1.100 -> 3.8332, 4.0633
// 6000, 1.100 -> 3.7915, 4.1138
~~~

### 7.6.3. Bias-variance tradeoff and model complexity

* <b>overfitting</b> (`high-variance`) occurs when the `ratio` of `model complexity` and `training-set size` (`model_complexity` / `training_set_size`) gets large </br>

    > 过拟合发生在（模型复杂度/样本数量）这个比值过大时

* If you have a complex model but also a relatively large training set, overfitting is less likely to occur. You saw that the RMSE on the validation set started to rise when you added higher-order polynomials and trained the model with more iterations. 

    > 当样本数量充足时，使用复杂的模型（如增加高阶特征、增加迭代次数），会发现测试集误差仍然可能可以继续降低

* Higher-order polynomials bring more complexity to the model, and more iterations overfit the model to the data while the algorithm is converging. If we try even more iterations after some point, the training RMSE continues to decrease while the testing RMSE continues to rise.

    > 当测试集误差降低到某个点位时，继续增加模型复杂度，（尽管训练集误差仍然继续降低）测试集误差会不降反生

~~~scala
scala> iterateLRwSGD(Array(10000, 15000, 30000, 50000), Array(1.1),
 trainHPScaled, validHPScaled)
10000, 1.100 -> 3.7638, 4.1553
15000, 1.100 -> 3.7441, 4.1922
30000, 1.100 -> 3.7173, 4.2626
50000, 1.100 -> 3.7039, 4.3163
~~~

### 7.6.4. Plotting residual plots

<b>`Residual`</b>: the difference between the predicted and actual values of the target variable. An ideal `Residual Plot` should have the same height at all points on the X-axis.

> <b>残差</b>：理想的残差可视化中、所有样本的残差都应当大致相同。</br>
> <b>残差可视化</b>：横坐标是验证集样本、纵坐标是残差
> 在本章的例子中，添加多项式特征会得到更好的Residual Plot虽然仍然不够理想、同时也可以发现一些离群点、这些离群点是样本存在缺失值导致的。</br>
> 有时残差可视化呈现出`fan-in`或者`fan-out`的形态，在这种情况下，除了添加多项式特征，对样本的y值（target variable）做对数变换有可能会有用。</br>

### 7.6.5. Avoiding overfitting by using regularization

`Regularization` adds an additional element (we denote it as β) to the cost function that penalizes complexity in the model, by increasing the error in proportion to absolute weight values. 

> 正则化：通过在`cost function`上添加正则项，例如`L1 Norm`(`Lasso`)和`L2 Norm`(`Ridge`) of the weight vector），当特征权重绝对值增大时，正则项会增加惩罚值，从而限制模型复杂度。其中L1正则项（`Lasso`）更加激进、倾向于让相关度低的特征权重归零

在模型中使用`Lasso`：

~~~scala
def iterateLasso(
        iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, 
        train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
    import org.apache.spark.mllib.regression.LassoWithSGD
    // 遍历超参数组合
    for(numIter <- iterNums; step <- stepSizes) {
        // 模型初始化
        val alg = new LassoWithSGD()      // 模型类型为LassoWithSGD
        alg.setIntercept(true)            // 需要训练得到截距
            .optimizer
            .setNumIterations(numIter)    // 迭代次数
            .setStepSize(step)            // 步长
            .setRegParam(regParam)        // 正则化参数
        // 模型训练
        val model = alg.run(train)    
        // 训练集、测试集预测结果及label
        val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
        val validPredicts     = test.map(x => (model.predict(x.features), x.label))
        // 训练集、测试集均方误差
        val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        // 打印均方误差
        println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
        println("\tweights: "+model.weights)
      }
}
iterateLasso(
    Array(200, 400, 1000, 3000, 6000, 10000, 15000),  // 迭代次数
    Array(1.1),        // 梯度下降步长
    0.01,              // L2正则项超参数
    trainHPScaled,     // 添加多项式特征的训练集
    validHPScaled      // 添加多项式特征的验证集
    )
//Our results:
// 200, 1.100 -> 4.1762, 4.0223
// 400, 1.100 -> 4.0632, 3.9964
// 1000, 1.100 -> 3.9496, 3.9987
// 3000, 1.100 -> 3.8636, 4.0362
// 6000, 1.100 -> 3.8239, 4.0705
// 10000, 1.100 -> 3.7985, 4.1014
// 15000, 1.100 -> 3.7806, 4.1304
~~~

在模型中使用`Ridge`：

~~~scala
def iterateRidge(
        iterNums:Array[Int],  stepSizes:Array[Double], regParam:Double, 
        train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
    import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
    // 遍历超参数组合
    for(numIter <- iterNums; step <- stepSizes) {
        // 模型初始化
        val alg = new RidgeRegressionWithSGD()    // 模型类型为RidgeRegressionWithSGD
        alg.setIntercept(true)                    // 需要训练得到截距
        alg.optimizer
            .setNumIterations(numIter)            // 迭代次数
            .setRegParam(regParam)                // 正则化超参数
            .setStepSize(step)                    // 步长
        // 训练模型        
        val model = alg.run(train)
        // 训练集、测试集预测结果及标签
        val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
        val validPredicts = test.map(x => (model.predict(x.features), x.label))
        // 训练集、测试集的均方误差
        val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    }
}
iterateRidge(Array(200, 400, 1000, 3000, 6000, 10000), Array(1.1), 0.01, trainHPScaled, validHPScaled)
// Our results:
// 200, 1.100 -> 4.2354, 4.0095
// 400, 1.100 -> 4.1355, 3.9790
// 1000, 1.100 -> 4.0425, 3.9661
// 3000, 1.100 -> 3.9842, 3.9695
// 6000, 1.100 -> 3.9674, 3.9728
// 10000, 1.100 -> 3.9607, 3.9745
~~~

### 7.6.6. K-fold cross-validation 

## 7. Optimizing linear regression

### 7.7.1. Mini-batch stochastic gradient descent (SGD)

<b>BGD</b></br>
> with `gradient descent` (also called `also called batch gradient descent`, `BGD`) as in previouse examples, the weights are updated in each step by going through the entire dataset</br>

<b>SGD</b></br>
> with `mini-batch stochastic gradient descent`  (`SGD`) , updating the weight only need to use a subset of the data in each step

~~~scala
def iterateLRwSGDBatch(
    iterNums:Array[Int], stepSizes:Array[Double], fractions:Array[Double], //超参数组合     train:RDD[LabeledPoint], test:RDD[LabeledPoint] //训练集、验证集
    ) = {
    // 遍历各个超参数组合
    for(numIter <- iterNums; step <- stepSizes; miniBFraction <- fractions) {
        // 初始化模型
        val alg = new LinearRegressionWithSGD() // 使用SGD
        alg.setIntercept(true)                  // 需要训练代表截距的参数
            .optimizer
            .setNumIterations(numIter)          // 迭代次数：通常比BGD需要更多迭代
            .setStepSize(step)                  // 梯度下降步长
        alg.optimizer.setMiniBatchFraction(miniBFraction) // Mini Batch的大小
        // 训练模型
        val model = alg.run(train)    
        // 训练集、预测集样本预测
        val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
        val validPredicts = test.map(x => (model.predict(x.features), x.label))
        // 训练集、预测集均方误差
        val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        println("%d, %5.3f %5.3f -> %.4f, %.4f".format(numIter, step, miniBFraction, meanSquared, meanSquaredValid))
  }
}
// 首先看一下step size超参数，Grid-Search之后发现0.4最好
// First, to get a feeling for the step-size parameter in the context of the other two
iterateLRwSGDBatch(
    Array(400, 1000),         // 迭代次数
    Array(0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1), //梯度下降步长
    Array(0.01, 0.1),         // mini-batch size
    trainHPScaled, validHPScaled
    )
//Our results:
// 400, 0.050 0.010 -> 6.0134, 5.2776
// 400, 0.050 0.100 -> 5.8968, 4.9389
// ...
// 400, 0.350 0.010 -> 5.3556, 5.7160
// 400, 0.350 0.100 -> 4.5129, 4.0734
// 400, 0.400 0.010 -> 5.4826, 5.8964
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.500 0.010 -> 16.2895, 17.5951
// 400, 0.500 0.100 -> 5.0948, 4.6498
// 400, 1.000 0.010 -> 332757322790.5126, 346531473220.8046
// 400, 1.000 0.100 -> 179186352.6756, 189300221.1584
// 1000, 0.050 0.010 -> 5.0089, 4.5567
// 1000, 0.050 0.100 -> 4.9747, 4.3576
// ...
// 1000, 0.350 0.010 -> 4.5500, 4.5219
// 1000, 0.350 0.100 -> 4.3176, 4.0689
// 1000, 0.400 0.010 -> 4.4396, 4.3976
// 1000, 0.400 0.100 -> 4.2904, 4.0562
// 1000, 0.500 0.010 -> 11.6097, 12.0766
// 1000, 0.500 0.100 -> 4.5170, 4.3467
// 1000, 1.000 0.010 -> 67686532719.3362, 62690702177.4123
// 1000, 1.000 0.100 -> 103237131.4750, 119664651.1957

// 接下来固定step-size超参数为0.4，Grid-Search其他超参数
// 发现迭代次数2000时，验证集误差最小，比上一节的BGD模型都小
// 迭代次数达到5000时发生过拟合
iterateLRwSGDBatch(
    Array(400, 1000, 2000, 3000, 5000, 10000),  
    Array(0.4), 
    Array(0.1, 0.2, 0.4, 0.5, 0.6, 0.8), 
    trainHPScaled, validHPScaled)
// Our results:
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.400 0.200 -> 4.4509, 4.0288
// ...
// 1000, 0.400 0.600 -> 4.2209, 4.0357
// 1000, 0.400 0.800 -> 4.2139, 4.0173
// 2000, 0.400 0.100 -> 4.1367, 3.9843
// 2000, 0.400 0.200 -> 4.1030, 3.9847
// 2000, 0.400 0.400 -> 4.1129, 3.9736
// 2000, 0.400 0.500 -> 4.0934, 3.9652
// 2000, 0.400 0.600 -> 4.0926, 3.9849
// 2000, 0.400 0.800 -> 4.0893, 3.9793
// 3000, 0.400 0.100 -> 4.0677, 3.9342
// 3000, 0.400 0.200 -> 4.0366, 4.0256
// ...
// 3000, 0.400 0.800 -> 4.0253, 3.9754
// 5000, 0.400 0.100 -> 3.9826, 3.9597
// 5000, 0.400 0.200 -> 3.9653, 4.0212
// 5000, 0.400 0.400 -> 3.9654, 3.9801
// 5000, 0.400 0.500 -> 3.9600, 3.9780
// 5000, 0.400 0.600 -> 3.9591, 3.9774
// 5000, 0.400 0.800 -> 3.9585, 3.9761
// 10000, 0.400 0.100 -> 3.9020, 3.9701
// 10000, 0.400 0.200 -> 3.8927, 4.0307
// 10000, 0.400 0.400 -> 3.8920, 3.9958
// 10000, 0.400 0.500 -> 3.8900, 4.0092
// 10000, 0.400 0.600 -> 3.8895, 4.0061
// 10000, 0.400 0.800 -> 3.8895, 4.0199
~~~

### 7.7.2. LBFGS optimizer

> LBFGS is a limited-memory approximation of the Broyden-Fletcher-Goldfarb-Shanno (BFGS) algorithm for minimizing multidimensional functions. The classic BFGS algorithm calculates an approximate inverse of the so-called Hessian matrix, which is a matrix of second-degree derivatives of a function, and keeps an n × n matrix in memory, where n is the number of dimensions. LBFGS keeps fewer than 10 of the last-calculated corrections and is more memory efficient, especially for larger numbers of dimensions. It stops if the RMSE after each iteration changes less than the value of the convergence-tolerance parameter. This is a much more natural and more simple criterion.

> `LBFGS`更加节省内存，可用于维度较多的数据集，根据误差收敛程度来决定是否停止训练，可以在到达max iterations之前提前停止

~~~scala
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("breeze").setLevel(Level.WARN)

def iterateLBFGS(
    regParams:Array[Double],  // L2正则化超参数
    numCorrections:Int,       // 保留最近多少轮的
    tolerance:Double,         // 早期停止阈值（收敛值容忍度）
    train:RDD[LabeledPoint],  // 训练集
    test:RDD[LabeledPoint]    // 验证集
) = {
    import org.apache.spark.mllib.optimization.LeastSquaresGradient
    import org.apache.spark.mllib.optimization.SquaredL2Updater
    import org.apache.spark.mllib.optimization.LBFGS
    import org.apache.spark.mllib.util.MLUtils
    
    val dimnum = train.first().features.size    // 特征
    for(regParam <- regParams) {
        val (weights:Vector, loss:Array[Double]) = LBFGS.runLBFGS(
            train.map(x => (x.label, MLUtils.appendBias(x.features))),  // 训练集
                  new LeastSquaresGradient(),   // 损失函数？
                  new SquaredL2Updater(),       // 使用L2正则项?
                  numCorrections,               // 最多保留多少轮
                  tolerance,                    // 模型收敛度阈值
                  50000,                        // 最多训练多少轮（不收敛时强制模型停止）
                  regParam,                     // L2正则化超参数
                  Vectors.zeros(dimnum+1))
        
        val model = new LinearRegressionModel(
            Vectors.dense(weights.toArray.slice(0, weights.size - 1)),
            weights(weights.size - 1))
        // 在训练集、验证集上预测，得到训练集验证集的误差值
        val trainPredicts = train.map(x => (model.predict(x.features), x.label))
        val validPredicts = test.map(x => (model.predict(x.features), x.label))
        val meanSquared = math.sqrt(
            trainPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        val meanSquaredValid = math.sqrt(
            validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
        println("%5.3f, %d -> %.4f, %.4f".format(regParam, numCorrections, meanSquared, meanSquaredValid))
  }
}

// 模型收敛速度很快，主要需要
iterateLBFGS(
    Array(0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1),  // L2正则项超参数
    10,       // number of corrections to keep (should <= 10, 10 is default)
    1e-5,     // converge tolerance
    trainHPScaled, 
    validHPScaled
    )
//Our results:
// 0.005, 10 -> 3.8335, 4.0383
// 0.007, 10 -> 3.8848, 4.0005
// 0.010, 10 -> 3.9542, 3.9798
// 0.020, 10 -> 4.1388, 3.9662
// 0.030, 10 -> 4.2892, 3.9996
// 0.050, 10 -> 4.5319, 4.0796
// 0.100, 10 -> 5.0571, 4.3579
~~~
