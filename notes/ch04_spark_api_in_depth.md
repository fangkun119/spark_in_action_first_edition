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

1. <b>`Ordered trais`</b>: similar to Java's `Comparable` interface

> create the class by extending `Ordered` trait which can compare with another object

2. <b>`Ordering trait`</b>: similar to java's `Comparator` interface

> create a class which provide a function to compare 2 RDD objects









## 4.4 Understanding RDD dependencies


## 4.5 Using accumulators and broadcast variables to communicate with Spark executors


## 4.6 Summary

