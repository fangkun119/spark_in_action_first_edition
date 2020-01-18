# Ch04 The Spark API in depth

* 4.1. Working with pair RDDs
* 4.2. Understanding data partitioning and reducing data shuffling
* 4.3. Joining, sorting, and grouping data
* 4.4. Understanding RDD dependencies
* 4.5. Using accumulators and broadcast variables to communicate with Spark executors
* 4.6. Summary

## 4.1 Working with pair RDDs

<b>`pair RDDS`</b>:RDDs containing key-value tuples<br/>

### 4.1.1 Creating pair RDDs 

Approach 1: `SparkContext` produce pair RDDs by default (for example, methods for reading files in HDFS formats)
Approach 2: use a `keyBy` transformation, which accept a function for generating keys
Approach 3: convert a normal RDD to pair RDDs like this code "`var transByCust = tranData.map(tran => (tran(2).toInt, tran))`"

> Comments: <br/>
> 1. Pair RDD functions in `PairRDDFunctions` will be automatically become available by implicit conversion (CH02)<br/>
> 2. [creating pair RDDS in Java](https://livebook.manning.com/book/spark-in-action/chapter-4/12)<br/>

### 4.1.2 Basic pair RDD functions

#### requirement, files and environment setup

[https://livebook.manning.com/book/spark-in-action/chapter-4/18](https://livebook.manning.com/book/spark-in-action/chapter-4/18)<br/>

Requirement: adding complimentary products to customers:<br/>

> 1. A bear doll: customer who made the most transactions<br/>
> 2. 5% discount: for two or more Barbie Shopping Mall Playsets bought<br/>
> 3. A toothbrush: for more than five dictionaries bought<br/>
> 4. A pair of pajamas: to the customer who spent the most money overall<br/>

<b>Data File Format</b>: <br/>
Each line contains a transaction, fields includes: `date`, `time`, `customer ID`, `product ID`, `quantity`, `product price`, delimited with hash signs<br/>

#### create the Pair RDD

~~~shell
scala> val tranFile = sc.textFile("first-edition/ch04/" + "ch04_data_transactions.txt")
scala> val tranData = tranFile.map(_.split("#")) //customer_id is in column[2]
scala> var transByCust = tranData.map(tran => (tran(2).toInt, tran))
scala> transByCust.keys.distinct().count() //to check the keys
res0: Long = 100
~~~

> Alternative way of `PairRDDFunctions.keys()` transformation: `map(_._1)`<br/>
> Alternative way of `PairRDDFunctions.values()` transformation: `map(_._2)`<br/>

#### counting Values per key

~~~shell
scala> transByCust.countByKey()
res1: scala.collection.Map[Int,Long] = Map(69 -> 7, 88 -> 5, 5 -> 11,
10 -> 7, 56 -> 17, 42 -> 7, 24 -> 9, 37 -> 7, 25 -> 12, 52 -> 9, 14 -> 8,
20 -> 8, 46 -> 9, 93 -> 12, 57 -> 8, 78 -> 11, 29 -> 9, 84 -> 9, 61 -> 8,
89 -> 9, 1 -> 9, 74 -> 11, 6 -> 7, 60 -> 4,...
scala> transByCust.countByKey().values.sum
res3: Long = 1000
~~~

> `RDDFunctions.countByKey()` is an `RDD action`, unlike `RDD transformation`, `RDD action` immediately return the result as a Java/Scala/Python object rather another RDD.

find the customer who made the most purcheses

~~~shell
scala> val (cid, purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
cid: Int = 53
purch: Long = 19
~~~

the complimetary bear dool (product ID: 4) for this customer (customer ID: 53)

~~~shell
scala> var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4",
"1", "0.00"))
~~~

#### looking up values for a single key

~~~shell
scala> // all the product purchased by customer with ID 53
scala> transByCust.lookup(53)
res1: Seq[Array[String]] = WrappedArray(Array(2015-03-30, 6:18 AM, 53, 42,
5, 2197.85), Array(2015-03-30, 4:42 AM, 53, 3, 6, 9182.08), ...
scala> // print
scala> transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))
2015-03-30, 6:18 AM, 53, 42, 5, 2197.85
2015-03-30, 4:42 AM, 53, 3, 6, 9182.08
...
~~~

`WrappedArray` (returned by the `lookup` transformation) is Scala's way to present an `Array` as a `Seq`(mutable sequence) object through implicit conversion<br/>

#### using the mapValues transformation to change values in a pair RDD

~~~shell
scala> // give a 5% discount for two or more Barbie Shopping Mall Playsets bought
scala> // 25: Barbie Shopping Mall Playset ID
scala> transByCust = transByCust.mapValues(tran => {
     if(tran(3).toInt == 25 && tran(4).toDouble > 1)
         tran(5) = (tran(5).toDouble * 0.95).toString
     tran })
~~~

#### using the flatMapValues transformation to add values to keys

`transByCust` schema

~~~shell
scala> // all transactions of a customer (each inner Array is a transaction)
scala> transByCust.lookup(53)
res1: Seq[Array[String]] = WrappedArray(Array(2015-03-30, 6:18 AM, 53, 42,
5, 2197.85), Array(2015-03-30, 4:42 AM, 53, 3, 6, 9182.08), ...

scala> // `date`, `time`, `customer ID`, `product ID`, `quantity`, `product price`
scala> transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))
2015-03-30, 6:18 AM, 53, 42, 5, 2197.85
2015-03-30, 4:42 AM, 53, 3, 6, 9182.08
...
~~~

example: 

~~~shell
scala> // add a complimentary toothbrush (ID 70) to customers who bought five or more dictionaries (ID 81)
scala> // flatMap concanated the WrapArrays, thus each element is a inner array which represent a transaction
scala> // tran is an anonymous functions
scala> transByCust = transByCust.flatMapValues(tran => { 
	 //3:product ID; 81: dictionary; 4; quantity; 5:product price
    if(tran(3).toInt == 81 && tran(4).toDouble >= 5) {
       val cloned = tran.clone()
       cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
       List(tran, cloned)
    } 
    else 
       List(tran)
    })
~~~

#### Using the `RDD.reduceByKey`/`RDD.foldByKey` to merge values of ONE key

`RDD.reduceByKey` operation merge all the values of <b>a</b> key into a single value of the same type, according to the `merge` function passed in<br/>

`RDD.foldByeKey' operation does the same thing, except that it requires an additional parameter `zeroValue`<br/>

> `zeroValue` should be a neutral value (0 for addition, 1 for multiplication, Nil for lists, and so forth), it applied on the first value of a key and might be applied multiple times due to the RDD's parallel nature.

~~~scala
foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
~~~

example: 

~~~shell
scala> // finding the customer who spent the most
scala> val amounts = transByCust.mapValues(t => t(5).toDouble)
scala> val totals = amounts.foldByKey(0)((p1, p2) => p1 + p2).collect()
res0: Array[(String, Double)] = Array((84,53020.619999999995),
(96,36928.57), (66,52130.01), (54,36307.04), ...
scala> totals.toSeq.sortBy(_._2).last
res1: (Int, Double) = (76,100049.0)

scala> // give a pair of pajamas (ID 63) to the customer with ID 76 
scala> complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76",
"63", "1", "0.00")

scala> // add complTrans to the transByCust RDD and save to file
scala> // sc.parallelize(local_python_collection): convert local collection to RDD
scala> // t(2): column 2 is customer ID
scala> transByCust = transByCust.union(sc.parallelize(complTrans).map(t =>
(t(2).toInt, t)))
scala> transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-
transByCust")
~~~

#### Using `RDD.aggregateByKey` to group all values of a key

`RDD.aggregateByKey` is similar to `RDD.foldByKey` and `RDD.reduceByKey` but takes another parameter in addation to the `zeroValue` argument. All the 3 paremeters are: 

1. `a transform function` for gtransforming values from type `v` to `u` (`(U,V)=>V`) 
2. `a merge function` for merging the transformed values (`(U, U) => U`) 
3. `zeroValue` argument

> `aggregateByKey` returns a parameterized function that takes the other two arguments. But you wouldn’t normally use `aggregateByKey` like that; you would provide both sets of parameters at the same time
> The double parameter list in `1.` and `2.` is a Scala feature known as `currying` ([www.scala-lang.org/old/node/135](www.scala-lang.org/old/node/135))

API: 

~~~scala
def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): RDD[(K, U)]
Aggregate the values of each key, using given combine functions and a neutral "zero value".
~~~

example: get a list of all products your customers purchased

~~~shell
scala> // trans(3) is customer_id
scala> // zeroValue List[String](), an empty string
scala> // seqOp (U, V) => (U): (prods, tran) => prods ::: List(tran(3)) 
scala> // combOp (U, U) => (U): (prods1, prods2) => prods1 :: prods2
scala> val prods = transByCust.aggregateByKey(List[String]())(
   (prods, tran) => prods ::: List(tran(3)),
   (prods1, prods2) => prods1 ::: prods2)
scala> prods.collect()
res0: Array[(String, List[String])] = Array((88,List(47.149.147.123, 
74.211.5.196,...), (82,List(8.140.151.84, 23.130.185.187,...), ...)
~~~

comment: `:::` is a operator for concatenating two lists

## 4.2 Understanding data partitioning and reducing data shuffling

`partition`: each part (piece or slice) of an RDD is called a partition, each RDD maintains a list of its partitions and an optional list of prefered locations for computing the partitions

comments: 

> Partitions were previously called splits. The term split can still be found in Spark’s source code<br/>
> The list of an RDD’s partitions can be obtained from that RDD’s `partitions` field

### 4.2.1 using Spark's data partitioners

Partitioning is performed by `Partioner` objects (assigning index to each element), two implementions are `HashPartioner`(default) and `RangePartitioner`

<b>`HashPartioner`</b>

default number of partitions: `spark.default.parallelism` (CH12)

<b>`RangePartitioner`</b>

stores RDDs into roughly equal ranges by sampling to determines the range boundaries

<b>Pair RDD custom partitioners</b>

> for example: you want each task processes only a specific subset of key-value pairs, where all of them belong to a single database, single database table, single use

`Custom partitioners` can be used only on `pair RDDs`, by passing them to any `pair RDD transformations` except `mapValues` and `flatMapValues`. For example: 

~~~scala
rdd.foldByKey(afunction) //max num of partitions of parent RDDs which transformed into this one (or spark.default.parallelism if no parent)
rdd.foldByKey(afunction, 100) //identitical with using `new HashPartitioner(100)`
rdd.foldByKey(afunction, new HashPartitioner(100)) //this version could pass in customer partitioner
~~~

> another approach is still using the default `HashPartitioner` but change the key's hash code

### 4.2.2 understanding and avoiding unnecessary shuffling

> visualizing partitions during a shuffle in transformation by example of `aggregateByKey` transformation: [https://livebook.manning.com/book/spark-in-action/chapter-4/118](https://livebook.manning.com/book/spark-in-action/chapter-4/118)

~~~shell
scala> val prods = transByCust.aggregateByKey(List[String]())(
   (prods, tran) => prods ::: List(tran(3)),
   (prods1, prods2) => prods1 ::: prods2)
~~~

problem: how to minimize the number of shuffles during the job above<br/>
there are 3 kinds of conditions that triggering the shuffles, we solve them one by one

<b>Shuffling when explicitly changing partitioners</b>

* `Pair RDD` custom partitioners, `shuffling` always occures
* `Shuffling` also occurs if a different `HashPartitioner` then the previous on is used

examples that cause `shuffling`

~~~scala
// assume RDD's parallelism is different than 100
rdd.aggregateByKey(zeroValue, 100)(seqFunc, comboFunc).collect() 
rdd.aggregateByKey(zeroValue, new CustomPartitioner())(seqFunc,
 comboFunc).collect()
~~~

tips: use default partitioner as much as possible to avoid this kind of suffling

<b>Shuffle caused by partitioner removal</b>

`map` and `flatMap` transformations some times cause shuffling, such as transform the resulting RDD as the `reduceByKey` in 3rd line of below code

~~~shell
scala> val rdd:RDD[Int] = sc.parallelize(1 to 10000) 
// creates a pair RDD by using the map transformation, which removes the partitioner, and then switches its keys and values by using another map transformation
// this line don't caused a shuffle
scala> rdd.map(x => (x, x*x)).map(_.swap).count()
// uses the same pair RDD as before, but this time the reduceByKey transformation  instigates a shuffle
scala> rdd.map(x => (x, x*x)).reduceByKey((v1, v2)=>v1+v2).count()
~~~

complete list of transformations that cause a shuffle after `map` or `flatMap` transformations: 

* Pair RDD transformations that can change the RDD’s partitioner: `aggregateByKey`, `foldByKey`, `reduceByKey`, `groupByKey`, `join`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin`, and `subtractByKey`
* RDD transformations: `subtract`, `intersection`, and `groupWith`
* `sortByKey` transformation (which always causes a shuffle)
* `partitionBy` or `coalesce` with `shuffle=true` (covered in the next section)

<b>optimizing shuffling with an external shuffle service</b>

> If some executors get killed, other executors can no longer get shuffle data from them, and the data flow is interrupted. This can be solved by external shuffle server

if setting `spark.shuffle.service.enabled` to `true`, one external shuffle server is started per worker node

<b>parameters that affect shuffling</b>

`spark.shuffle.manager parameter`(`hash`, `sort`): specifies shuffle implementation

> [http://mng.bz/s6EA](http://mng.bz/s6EA)

`spark.shuffle.consolidateFiles`(`true`, `false`): specifies whether to consolidate intermediate files, recomend change this to `true` (default is `false`)

> [http://http://mng.bz/O304](http://mng.bz/O304)

`spark.shuffle.spill`(`true`,`false`): specifies whether the amount of memory used for these taskes should be limitted, any excess data will spill over to disk (default is `true`)

`spark.shuffle.memoryFraction`: specified the memory limit, too high will cause OOM exception, too low will cause frequently spilling

`spark.shuffle.spill.compress`: specified whether to compress intermediate files (default is `true`)

`spark.shuffle.spill.batchSize`: number of objects serialized/deserialized together when spilling to disk (default is 10000)

`spark.shuffle.service.port`: external shuffle service port if the service is enabled

### 4.2.3. Repartitioning RDDs

> explicitly repartition an RDD in order to distribute the workload more efficiently or avoid memory problems <br/>
> Some Spark operations, for example, default to a small number of partitions, which results in partitions that have too many elements (take too much memory) and don’t offer adequate parallelism <br/>

API: `RDD.partitionBy`, `RDD.coalesce`, `repartition`, `repartitionAndSortWithinPartition`

#### `RDD.PartitionBy`

available only on `Pair RDDs`, accept only one parameter, the desired `Partitioner` object

#### `RDD.coalesce` and `RDD.repartition`

<b>`coalesce`</b> is used for reducing or increasing the number of partitions 

method signature is as below, the 2nd parameter is for specifying whether need to `shuffle`

~~~scala
`coalesce (numPartitions: Int, shuffle: Boolean = false)`
~~~

<b>`repartitioning`</b> balances new partitions so they're based on the same number of parent partitions, prefreed locality as much as bpssible but also trying to balance partitions across the machines

#### `RDD.repartitionAndSortWithinPartition`

Available only on `sortable RDDs` (pair RDDs with sortable keys). It repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys.

~~~scala
repartitionAndSortWithinPartitions(numPartitions=None, partitionFunc=<function portable_hash>, ascending=True, keyfunc=<function RDD.<lambda>>)[source]
~~~

### 2.4.2 Mapping data in partitions

Transforamtions that apply a function not to an RDD as a whole, but to each of its partitions separately

> since it works within 1 partition, shuffle can be avoided

API: `mapPartitions`, `mapPartitionsWithIndex`, `glom`

#### mapPartitions and mapPartitionsWithIndex

`mapPartitions` accepts a mapping function (similar to `map`, but need  a signature `Iterator[T]=>Iterator[U]`) for iterating over elements within each partition and creating partitions for the new RDD

~~~scala
def mapPartitions[U](f: (Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
Return a new RDD by applying a function to each partition of this RDD
~~~

`mapPartitionsWithIndex` accepts a mapping function (aslo need accepts the partition's index: `(Int, Iterator[T]) => Iterator[U]) 

~~~scala
def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) ⇒ Iterator[U], preservesPartitioning: Boolean = false)(implicit arg0: ClassTag[U]): RDD[U]
Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.
~~~

Mapping partitions can solve problems such as : opening a database connection for each partition rather than for each element

more reference: [http://mng.bz/CWA6](http://mng.bz/CWA6)
book: [https://livebook.manning.com/book/spark-in-action/chapter-4/180](https://livebook.manning.com/book/spark-in-action/chapter-4/180)

#### glom for collecting partition data

<b>`glom` (means grab)</b>: gathers elements of each partition into an array and returns a new RDD

~~~shell
scala> val list = List.fill(500)(scala.util.Random.nextInt(100))
list: List[Int] = List(88, 59, 78, 94, 34, 47, 49, 31, 84, 47, ...)
scala> val rdd = sc.parallelize(list, 30).glom()
rdd: org.apache.spark.rdd.RDD[Array[Int]] = MapPartitionsRDD[0]
scala> rdd.collect()
res0: Array[Array[Int]] = Array(Array(88, 59, 78, 94,...), ...)
scala> rdd.count()
res1: Long = 30
~~~

## 4.3 Joining, sorting, and grouping data

Request: 

1. names of products with totals sold, sorted alphabetically
2. a list of products the company didn’t sell yesterday
3. some statistics about yesterday’s transactions per customer: average, maximum, minimum, and the total price of products bought

Columns in `tranData`: <br/>
fields includes: `date`, `time`, `customer ID`, `product ID`, `quantity`, `product price`, delimited with hash signs

### 4.3.1 Joining data

data to be joined:

~~~scala
// key the transactions by product_id, tran(3) is product_id
// PairRDD: prod_id -> tran
val transByProd = tranData.map(tran => (tran(3).toInt, tran))

// calculate the total_quantity per product, tran(5) is quantity
// PairRDD: prod_id -> count
val totalsByProd = transByProd.mapValues(t => t(5).toDouble).
   reduceByKey{case(tot1, tot2) => tot1 + tot2}

// open another file to get product info
// PairRDD: prod_id -> product
val products = sc.textFile("first-edition/ch04/"+
    "ch04_data_products.txt").
    map(line => line.split("#")).
    map(p => (p(0).toInt, p))
~~~

`classic joins`,`zip`,`cartesian`,`intersection` can be used to join the data

#### `join`,`leftOuterJoin`,`rightOuterJoin`,`fullOuterJoin`: the classical join transformations

There are 4 classic join transformations: `join`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin`, they have samilar behavior like in the RMDB<br/>

* `join`: return `pair RDD` of `(K, (V,W))`, where `(V,W)` have all possible pairs from 2 RDDs that have the same keys
* `leftOuterJoin`: return `pair RDD` of `(K, (V, Option(W))`, where besides the output of `join`, the resulting RDD also have elements `(key, (v, None))` for keys only exists in 1st RDD
* `rightOuterJoin`: return `pair RDD` of `(K, (Option(V), W)`, where besides the output of `join`, the resulting RDD also have elements `(key, (v, None))` for keys only exists in 2nd RDD
* `fullOuterJoin`: return `pair RDD` of `(K, (Option(V), Option(W))`, the resulting RDD contains both `(key, (v, None))` and `(key, (None, w))` elements for those keys that exists only in one of the two RDDs

`Option`: [http://www.scala-lang.org/api/2.13.1/scala/Option.html](http://www.scala-lang.org/api/2.13.1/scala/Option.html)

> Scala’s way to avoid `NullPointerException`<br/>
> Can be either a None or a Some object</br>
> `Option.isEmpty`: check whether has a value</br>
> `Option.get`: get the value, exception if None</br>
> `Option.getOrElse(default)`: return the `default` expression if none, or `get` otherwise

Comments: <br/>
* If RDDs being joined have duplicate keys, these elements will be joined multiple times
* Partitioner: 
	* 1st priority is `Partitioner` passed into the `*join` transformation
	* 2st priority is the left RDD's Partition
	* 3rd priority is the right RDD's
	* 4th priority is a new `HashPartitioner` with partition number of `spark.default.partitions`

~~~scala
// left : prod_id -> prod_count
// right: prod_id -> prod_info
// join : prod_id -> (prod_count, prod_info)
scala> val totalsAndProds = totalsByProd.join(products)
scala> totalsAndProds.first()
res0: (Int, (Double, Array[String])) = (84,(75192.53,Array(84, Cyanocobalamin, 2044.61, 8)))

// the list of products the company didn't sell yesterday
// val totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
val totalsWithMissingProds = totalsByProd.rightOuterJoin(products)
val missingProds = totalsWithMissingProds.
  filter(x => x._2._1 == None).
  map(x => x._2._2)
missingProds.foreach(p => println(p.mkString(", ")))
// 43, Tomb Raider PC, 2718.14, 1
// 63, Pajamas, 8131.85, 3
// 3, Cute baby doll, battery, 1808.79, 2
// 20, LEGO Elves, 4589.79, 4
~~~

<b>Using `subtract` and `subtractByKey` transformations to remove common values</b>

`substract`: return elements from 1st RDD that no in 2nd RDD<br/>

`substractByKey`: return a `pair RDD` with pairs from the 1st `pair RDD` whose keys are not in the 2nd `pair RDD`

~~~scala
val missingProds = products.subtractByKey(totalsByProd).values
missingProds.foreach(p => println(p.mkString(", ")))
// 20, LEGO Elves, 4589.79, 4
// 3, Cute baby doll, battery, 1808.79, 2
// 43, Tomb Raider PC, 2718.14, 1
// 63, Pajamas, 8131.85, 3
~~~

> both of these 2 transform have 2 additional versions for set up `Partitioner`

#### `cogroup` transformation for joining RDDs

<b>`cogroup`</b> performs a grouping of values from <b>serveral RDDs</b> by <b>key</b> and returns an RDD whose <b>values</b> are <b>array of `Iterable` ojbects</b> (each `Iterable` object contains values from one RDD)

> `cogroup`: (1) groups values of several RDDs by key; (2) joins these grouped RDDs

API: 

~~~scala
cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]):
  RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
~~~

example: 

~~~shell
// totalsByProd: prod_id -> prod_count
// products: prod_id -> prod
// prodTotCogroup: prod_id -> (Iterable(prod_count), Iterable(prod))
scala> val prodTotCogroup = totalsByProd.cogroup(products)
// prodTotCogroup: org.apache.spark.rdd.RDD[(Int, (Iterable[Double],
// Iterable[Array[String]]))]...
~~~

If one of the 2 RDDs doen't contain one of the keys, the corresponding iterator will be empty

~~~shell
// x._2._1: iterator with matching values from totalsByProd (totals as Doubles), used as a filter
// x._2._2: iterator with products as Arrays of Strings
scala> prodTotCogroup
	.filter(x => x._2._1.isEmpty)
	.foreach(x => println(x._2._2.head.mkString(", ")))
// 43, Tomb Raider PC, 2718.14, 1
// 63, Pajamas, 8131.85, 3
// 3, Cute baby doll, battery, 1808.79, 2
// 20, LEGO Elves, 4589.79, 4

// prodTotCogroup: prod_id -> (Iterable(prod_count), Iterable(prod))
// .filter: Iterable(prod_count) not empty
// .map: prod_id, prod<prod_id>[0].prod_count, prod<prod_id>[0].prod
val totalsAndProds = prodTotCogroup
	.filter(x => !x._2._1.isEmpty) 
	.map(x => (x._2._2.head(0).toInt, (x._2._1.head, x._2._2.head)))
~~~

`x._2._1` selects the iterator with matching values from totalsByProd (totals as Doubles)<br/>
`x._2._2` selects the iterator with products as Arrays of Strings<br/>
`x._2._2.head` 1st element of Iterable(prod) <br/>
`x._2._2.head(0)` product_id (1st column) of 1st element(.head) of Iterable(prod)

#### `intersection` transformation

<b>`intersection`</b>: return a new RDD contains elements present in both RDDs

~~~scala
// totalsByProd: prod_id -> prod_count
// return: intersection of prod_id of totalsByProd and products
totalsByProd.map(_._1).intersection(products.map(_._1))
~~~

> `intersection` has alternative versions for setting partitions

#### `cartesian` transformation for combining 2 RDDs

<b>`cartesian`</b> transformation makes a cartesian product (笛卡尔积）of 2 RDDs in the form of an RDD of tuples (T,U)

~~~scala
val rdd1 = sc.parallelize(List(7,8,9))
val rdd2 = sc.parallelize(List(1,2,3))
rdd1.cartesian(rdd2).collect()
// res0: Array[(Int, Int)] = Array((7,1), (7,2), (7,3), (8,1), (9,1), (8,2),
(8,3), (9,2), (9,3))
~~~

> will has data transfor among all partitions and produce exponential number of elements, use a lot of memory

~~~scala
rdd1.cartesian(rdd2).filter(el => el._1 % el._2 == 0).collect()
// res1: Array[(Int, Int)] = Array((7,1), (8,1), (9,1), (8,2), (9,3))
~~~

#### `zip` transformation for joining RDDs

<b>`zip` transfromation</b>: behavior is like the "Clothes zipper", which is the same as Scalar's `zip` function, <b>but will throw an error if 2 RDDs don't have same number of partitions, and same number of elements in them</b>. 

> It is safe if one RDD is a result of a `map` transformation on the other one
> another function `zipPartitions` do not have such strict limitation (only require partition numbers are the same

example: 

~~~scala
val rdd1 = sc.parallelize(List(1,2,3))
val rdd2 = sc.parallelize(List("n4","n5","n6"))
rdd1.zip(rdd2).collect()
// res1: Array[(Int, Int)] = Array((1,"n4"), (2,"n5"), (3,"n6"))
~~~

#### `zipPartitions` transformation for joining RDDs

<b>`zipPartitions`</b> is for `zip` several RDDs by which enables you to iterate over elements in partitions

> like `mapPartitions` but it's for `zip` not execute some `map` function

<b>parameters</b>: two sets of arguments:

* 1st set: RDDs, and optional arguement `preservesPartitioning` to disable shuffle</br>
* 2nd set: a functions takes matching number of Iterator objects for accessing elements in each partition (must be able to handle RDDs with different element numbers)</br>

example: 

~~~Scala
val rdd1 = sc.parallelize(1 to 10, 10)                 // 10 integers in 10 partitions
val rdd2 = sc.parallelize((1 to 8).map(x=>"n"+x), 10)  // 8  strings  in 10 partitions
rdd1.zipPartitions(rdd2, true)((iter1, iter2) => {     // preservesPartitioning = true
         iter1
	.zipAll(iter2, -1, "empty")
        .map({case(x1, x2)=>x1+"-"+x2})
    }).collect()
//     res1: Array[String] = Array(1-empty, 2-n1, 3-n2, 4-n3, 5-n4, 6-empty, 7-n5, 8-n6, 9-n7, 10-n8)
~~~

Scala's `zipAll` function aboveis to combine 2 iterators with dummy value -1 and "empty" for missing values in the 2 RDDs

> also can change the number of elements in the partitions by using an iterator function such as `drop` or `flatMap`

### 4.3.2 sorting data

transformations: `sortByKey`, `sortBy`, `repartitionAndSortWithinPartition` (section 4.2.3)

#### sortBy

example: using `sortBy` to sort `totalsAndProds` alphabetically

~~~Scala
// sortedProds: prod_id -> (prod_count, prod_info), prod_info[1] is prod_name
val sortedProds = totalsAndProds.sortBy(_._2._2(1)) 
sortedProds.collect()
// output: 
// res0: Array[(Double, Array[String])] = Array((90,(48601.89,Array(90, AMBROSIA TRIFIDA POLLEN, 5887.49, 1))), (94,(31049.07,Array(94, ATOPALM MUSCLE AND JOINT, 1544.25, 7))), (87 (26047.72,Array(87, Acyclovir, 6252.58, 4))), ...

// identical approach
// 2 transforms: keyBy prod_name then sortByKey
val sortedProds2 = totalsAndProds.keyBy(_._2._2(1))sortByKey()
sortedProds2.collect()
~~~

> In `Java`, the `sortByKey` method takes an object that implements the [`Comparator` interface]( http://mng.bz/5Suh). That’s the standard way of sorting in Java. There is no `sortBy` method in the JavaRDD class.

#### sortByKey

Transformations `sortByKey` and `repartitionAndSortWithinPartition` care available only on `pair RDDs` with orderable keys. There are 2 ways to make a class orderable: 

1. <b>using `Ordered trais`</b>: similar to Java's `Comparable` interface

> create the class by extending `Ordered` trait which can compare with another object

~~~scala
case class Employee(lastName: String) extends Ordered[Employee] {
    // 0 for "=="; "<0" for "this < that"; ">0" for "this > that"
    override def compare(that: Employee) = this.lastName.compare(that.lastName)
}
~~~

> (1) `sortByKey` requires the parameter of type `Ordering`<br/>
> (2) `Employee` defined above is the type of `Ordered` however<br/>
> But it's OK because there's an implicit conversion in Scala from `Ordered` to `Ordering` to bridge them<br/>

2. <b>using `Ordering trait`</b>: similar to java's `Comparator` interface

> create a class which provide a function to compare 2 RDD objects

~~~scala
implicit val emplOrdering = new Ordering[Employee] {
    // 0 for "=="; "<0" for "left < right"; ">0" for "right > left"
    override def compare(a: Employee, b: Employee) =
         a.lastName.compare(b.lastName)
}
~~~

If `Employee.lastName` is of `simple type`, we can define it by the alternative way as below: 

~~~scala
// Employee.lastName must be of simple type within this approach
implicit val emplOrdering: Ordering[Employee] = Ordering.by(_.lastName)
~~~

#### `groupByKeyAndSortValues` for secondary sort

`groupByKeyAndSortValues` is new transformation 

* it is used on `pair RDD` `(K,V)`
* needs an implicit `Ordering[V]` object in the scope
* needs a partition parameter: `Partitioner` object or  `partition_num`
* return (K, Iterable(V)), for each Key give a Iterable(V) to with values sorted according to the implicit `Ordering` object

Alternative method from [https://issues.apache.org/jira/browse/SPARK-3655](https://issues.apache.org/jira/browse/SPARK-3655) which performing a secondary sort but without grouping: 

Detailed explaination is in: [https://livebook.manning.com/book/spark-in-action/chapter-4/305](https://livebook.manning.com/book/spark-in-action/chapter-4/305)

#### using `top` and `takeOrdered` to fetch sorted elements

`takeOrdered(n)` and `top(n)` to fetch the first or last `n` objects from an RDD 

> With the order sorted by an implicit `Ordering[T]` object defined in the scope</br>
> For `pariRDD`, it does not sort by `key`, but by the order vor (K,V) tuples</br>
> `takeOrdered(n)` and `top(n)` has been optimized and don't need to perform full sorting amoung partitions, and much faster than doing `sortBy` followed by `take`, but must make sure `n` is not too big for driver's memory

### 4.3.3 Grouping Data 

Transformations: `aggregateByKey` (section 4.1.2), `groupByKey`, `groupBy`, `combineByKey`

#### `groupByKey` and `groupBy`

<b>definations</b>

~~~scala
def groupByKey(): RDD[(K, Iterable[V])]
def groupBy[K](f: (T) => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
~~~

> they also has versions to add `Partitioner` or `partition_num` as parameter </br>
> `groupBy` provide a shortcut on `non-pair RDDs` by tranform them to a `pair RDDs`</br>

<b>examples</b>

~~~scala
rdd.map(x => (f(x), x)).groupByKey()
rdd.groupBy(f)
~~~

<b>these 2 transforms are memory expensive</b>, get all values of each key in memory

> recomend to use `aggregateByKey`, `reduceByKey`, `foldByKey` for simpler scenarios that don't need full-grouping

#### `combineByKey` 

`combineByKey` is a generic transformation allows specify 1 customer function for merging `values` into `combined values` and anther 1 customer function for merging `combined values` themselves

function signature: 

~~~scala
def combineByKey[C](
	createCombiner: V      	=> C,	// create the combined value (C) from the 1st key's value (V)
	mergeValue: (C, V)		=> C,	// merge another key's value (V) in the same partition into the combined value (C)
	mergeCombiners: (C, C)	=> C,	// merge combined values (C) among partitions
	partitioner: Partitioner,		// if is same as existing partitoner, no shuffle is needed
	mapSideCombine: Boolean = true,
	serializer: Serializer = null): RDD[(K, C)] // default is configed in spart.serializer
~~~

source code: [http://mng.bz/Z6fp](http://mng.bz/Z6fp) 

> about how `aggregateByKey`, `groupByKey`, `foldByKey`, `reduceByKey` is implemented with `combinedByKey`

exmaples: 

~~~scala
// create the combined value (output) from the 1st key's value (t)
// 		t(transaction): date, time, customer_id, prod_id, quantity, price
// 		t(t):   price
// 		t(4):   quantity
// input:  transaction
// output: (min_avg_price, max_avg_price, quantity, total)
def createComb = (
	t:Array[String]) => {
		val total = t(5).toDouble 
		val q = t(4).toInt
		(total/q, total/q, q, total)}

// merge another key's value (V) in the same partition into the combined value (output)
def mergeVal:(
	(Double,Double,Int,Double), Array[String]) => (Double,Double,Int,Double) = {
		case((mn,mx,c,tot), t) => {
			val total = t(5).toDouble
			val q = t(4).toInt
			(scala.math.min(mn,total/q), scala.math.max(mx,total/q), c+q, tot+total)
		}
	}

// merge combined values among partitions
def mergeComb:(
	(Double,Double,Int,Double), (Double,Double,Int,Double)) => (Double,Double,Int,Double) = {
		case((mn1,mx1,c1,tot1),(mn2,mx2,c2,tot2)) 
			=> (scala.math.min(mn1,mn1), scala.math.max(mx1,mx2), c1+c2, tot1+tot2)
	} 

// transByCust: customer_id -> Array(transaction<date,time,customer_id,prod_id, quantity, price>)
// invoking co
val avgByCust = transByCust.combineByKey(
		createComb, // for creating the combined value from the 1st key's value
		mergeVal,   // for merging another key's valuein the same partition into the combined value
		mergeComb,  // for merging combined values among partitions
		// the partitioning is preserved by using the previous number of partitions #4
	  	new org.apache.spark.HashPartitioner(transByCust.partitions.size)
	).mapValues(
		# function parsed to RDD.mapValues
	 	{case(mn,mx,cnt,tot) => (mn,mx,cnt,tot,tot/cnt)}
	) 
	
// test
avgByCust.first()
//		res0: (Int, (Double, Double, Int, Double, Double)) = (96,(856.2885714285715,4975.08,57,36928.57,647.869649122807))

// save to file
totalsAndProds
	.map(_._2)
	.map(x=>x._2.mkString("#")+", "+x._1)
	.saveAsTextFile("ch04output-totalsPerProd")

avgByCust
	.map{case (id, (min, max, cnt, tot, avg)) 
		=> "%d#%.2f#%.2f#%d#%.2f#%.2f".format(id, min, max, cnt, tot, avg)}
	.saveAsTextFile("ch04output-avgByCust")
~~~

## 4.4 Understanding RDD dependencies

>  Spark’s inner mechanisms: RDD dependencies and RDD checkpointing which complete the picture of the Spark Core API

### 4.4.1. RDD dependencies and Spark execution

#### execution model

* Spark’s execution model is based on directed acyclic graphs (DAGs: 有向无环图) <br/>
* RDDs are vertices and dependencies are edges <br/>
* Performing a transformation, will create a new vertex (a new RDD) and a new edge (a dependency)<br/>

#### dependencies

2 types of dependencies: `narrow dependencies` and `wide denpendencies (shuffle)` <br/>

> whether shuffle is needed is denpend on the rule in $4.2.2, also a shuffle is always performed when joining RDDs

`narrow dependencies` includes: 

* `one-to-one dependencies`: 
* `range dependencies`: used for `union` transformation which combining several parent RDDs

example is in [https://livebook.manning.com/book/spark-in-action/chapter-4/350](https://livebook.manning.com/book/spark-in-action/chapter-4/350)

~~~scala
val list = List.fill(500)(scala.util.Random.nextInt(10))
val listrdd = sc.parallelize(list, 5)
val pairs = listrdd.map(x => (x, x*x))
val reduced = pairs.reduceByKey((v1, v2)=>v1+v2)
val finalrdd = reduced.mapPartitions(
             iter => iter.map({case(k,v)=>"K="+k+",V="+v}))
finalrdd.collect()
~~~

print the RDD's DAG which is useful when tuning the performance or debugging

~~~shell
scala> println(finalrdd.toDebugString)
(6) MapPartitionsRDD[4] at mapPartitions at <console>:20 []
 |  ShuffledRDD[3] at reduceByKey at <console>:18 []
 +-(5) MapPartitionsRDD[2] at map at <console>:16 []
    |  ParallelCollectionRDD[1] at parallelize at <console>:14 []
~~~

### 4.2.2 Spark stages and tasks

this section is about how Spark packages work to be sent to executors

`Every job` is divided into stages based on the `points where shuffles occu` 

> For each stage and each partition, tasks are created and sent to the executors. If the stage ends with a shuffle, the tasks created will be shuffle-map tasks. After all tasks of a particular stage complete, the driver creates tasks for the next stage and sends them to the executors, and so on. This repeats until the last stage (in this case, Stage 2), which will need to return the results to the driver. The tasks created for the last stage are called result task

### 4.3.3 Saving the RDD lineage with checkpointing

`RDD lineage`: graph of dependencies of RDDs

Spark provides a way to persist (RDD data, caching, lineage on checkpointing) the entire RDD to stable storage for recovery on node failure. 

<b>`checkpoint` operation</b>: will trigger the checkpoint to the `HDFS path` or `local path` assigned by `SparkContext.setCheckpointDir()` 

> `checkpoint` is also importent in Spark Streaming


## 4.5 Accumulators and broadcast variables 

> `accumulators` and `broadcast` are used to communicate with Spark executors for maintaining global state or share data across tasks and partitions 

### 4.5.1 obtaining data from executors with accumulators

`accumulators`: (1) shared across executors (2) can only add to (such as global sums and counters)

#### Accumulator

<b>`SparkContext.accumulator(initialValue)`</b> or <b>`sc.accumulator(initialValue, "accumulatorName")`</b> can be used for creating accumulators, it will be displayed in `Spark web UI (stage details page), CH11` for tracking

example: 

~~~scala
val acc  = sc.accumulator(0, "acc name")
val list = sc.parallelize(1 to 1000000)
list.foreach(x => acc.add(1))

acc.value
// res0: Int = 1000000

list.foreach(x => acc.value)
// java.lang.UnsupportedOperationException: Can't read accumulator value in task
~~~

#### AccumulatorParam

`AccumulatorParam` defined in the scope of `Accumulator` or `Accumulable` is used for adding values to them 

> Spark provides implicit `AccumulatorParam` for `numeric types`, but for other types, need to provide a custom `AccumulatorParam` object

<b>methods of AccumulatorParam</b>

* `zero(initialValue: T)`: initial value passed to executors 
* `addInPlace(v1: T, v2: T):T`: merge 2 accumulated values
* `addAccumulator(v1: T, v2: V): T`: add value of an Accumulator to the accumulated value


#### Accumulable

`Accumulable` is for custom accumulators cooporated with `AccumulableParam`

example: 

~~~
val rdd = sc.parallelize(1 to 100)
import org.apache.spark.AccumulableParam

// in (Int, Int): 1st Int is for tracking count, 2nd is for sum
implicit object AvgAccParam extends AccumulableParam[(Int, Int), Int]
{
	def zero(v:(Int, Int)) = (0, 0)
	def addInPlace(v1:(Int, Int), v2:(Int, Int))
  		= (v1._1+v2._1, v1._2+v2._2)
	def addAccumulator(v1:(Int, Int), v2:Int)
  		= (v1._1+1, 	v1._2+v2)
}
val acc = sc.accumulable((0,0))
rdd.foreach(x => acc += x)
val mean = acc.value._2.toDouble / acc.value._1
// mean: Double = 50.5
~~~

#### accumulating values in accumulable collections

`SparkContext.accumulableCollection()` create a shared mutable collections for storing the accumulated values

> do no need implicit objects as in previouse section </br>
> easier to implement 

~~~scala
import scala.collection.mutable.MutableList
val colacc = sc.accumulableCollection(MutableList[Int]())

rdd.foreach(x => colacc += x)
colacc.value
//	res0: 
//		scala.collection.mutable.MutableList[Int]
//	 	= MutableList(1, 2, 3, 4, 5, 6, 7, 8, 9, 
//						 10, 31, 32, 33, ...)
~~~

<b>notice:</b> the results aren’t sorted

> because there’s no guarantee that the accumulator results from various partitions will return to the driver in any specific order

### 4.5.2 broadcast variables

`broadcast variables` is used for sending data to executors, the driver create it, and executors read it.

> for example: you have a large set of data that the majority of your executors need, `broadcast variables` don't cause duplicated data transfer

#### sending data to executors

`SparkContext.broadcast(value)` will create a `Broadcast` object

> the `value` <br/>
> can be any serializable object</br>
> it can be read by executors using the `Broadcast.value` method

<b>notice:</b> always access the content through `value` mehtod, and never directly for preventing duplicated serialize and vairable shipping 

#### destroying and unpersisting broadcast variables

`destroy` method removes the broadcast variable from both executors and driver

`unpersist` method only removes the broadcast variable from cache in the executor. The variable can be sent to executors again

broadcast variables are `automatically unpersisted` by Spark after they go out of scope

> so it’s unnecessary to explicitly unpersist them, instead, you can remove the reference to the broadcast variable in the driver program

#### configuration parameters 

* `spark.broadcast.compress`: whether compress before transfer (leave this at true)
* `spark.io.compression.codec`: codec for compress
* `spark.broadcast.blockSize`: size of the chunks of data used for transferring broadcast data. (probably leave this at the battle-tested default of 4096).
* `spark.python.worker.reuse`: greatly affect broadcast performance in Python because, if workers aren’t reused, broadcast variables will need to be transferred for each task (You should keep this at true, which is the default value) .

## 4.6 Summary

* `Pair RDDs` contain two-element tuples: `keys` and `values`.
* Pair RDDs in Scala are implicitly converted to instances of class PairRDDFunctions, which hosts special pair RDD operations.
* countByKey returns a map containing the number of occurrences of each key.
* mapValues changes the values contained in a pair RDD without changing the associated keys.
* flatMapValues enables you to change the number of elements corresponding to a key by mapping each value to zero or more values.
* reduceByKey and foldByKey let you merge all the values of a key into a single value of the same type.
* aggregateByKey merges values, but it also transforms values to another type.
* Data partitioning is Spark’s mechanism for dividing data between multiple nodes in a cluster.
* The number of RDD partitions is important because, in addition to influencing data distribution throughout the cluster, it also directly determines the number of tasks that will be running RDD transformations.
* Partitioning of RDDs is performed by Partitioner objects that assign a partition index to each RDD element. Spark provides two implementations: HashPartitioner and RangePartitioner.
* Physical movement of data between partitions is called shuffling. It occurs when data from multiple partitions needs to be combined in order to build partitions for a new RDD.
* During shuffling, in addition to being written to disk, the data is also sent over the network, so it’s important to try to minimize the number of shuffles during Spark jobs.
* RDD operations for working on partitions are mapPartitions and mapPartitionsWithIndex.
* The four classic joins in Spark function just like the RDBMS joins of the same names: join (inner join), leftOuterJoin, rightOuterJoin, and fullOuterJoin.
* cogroup performs grouping of values from several RDDs by key and returns an RDD whose values are arrays of Iterable objects containing values from each RDD.
* The main transformations for sorting RDD data are sortByKey, sortBy, and repartitionAndSortWithinPartition.
* Several pair RDD transformations can be used for grouping data in Spark: aggregateByKey, groupByKey (and the related groupBy), and combineByKey.
* RDD lineage is expressed as a directed acyclic graph (DAG) connecting an RDD with its parent RDDs, from which it was transformed.
* Every Spark job is divided into stages based on the points where shuffles occur.
* The RDD lineage can be saved with checkpointing.
* Accumulators and broadcast variables enable you to maintain a global state or share data across tasks and partitions in your Spark programs.