<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!--**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)**-->

- [CH04 Spark API深入](#ch04-spark-api%E6%B7%B1%E5%85%A5)
  - [4.1 `Pair RDDs`](#41-pair-rdds)
    - [4.1.1 创建`Pair RDD`](#411-%E5%88%9B%E5%BB%BApair-rdd)
    - [4.1.2 `Pair RDD`函数](#412-pair-rdd%E5%87%BD%E6%95%B0)
      - [(1) 背景](#1-%E8%83%8C%E6%99%AF)
        - [(a) Use Case](#a-use-case)
        - [(b) 数据](#b-%E6%95%B0%E6%8D%AE)
      - [(2) 创建`Pair RDD`](#2-%E5%88%9B%E5%BB%BApair-rdd)
      - [(3) Use Case 1：为交易量最大的客户赠送玩具熊（找到value数量最大的key）](#3-use-case-1%E4%B8%BA%E4%BA%A4%E6%98%93%E9%87%8F%E6%9C%80%E5%A4%A7%E7%9A%84%E5%AE%A2%E6%88%B7%E8%B5%A0%E9%80%81%E7%8E%A9%E5%85%B7%E7%86%8A%E6%89%BE%E5%88%B0value%E6%95%B0%E9%87%8F%E6%9C%80%E5%A4%A7%E7%9A%84key)
      - [(4) Use Case 2：为满足条件的客户提供5%的折扣（使用`mapValues`修改value）](#4-use-case-2%E4%B8%BA%E6%BB%A1%E8%B6%B3%E6%9D%A1%E4%BB%B6%E7%9A%84%E5%AE%A2%E6%88%B7%E6%8F%90%E4%BE%9B5%25%E7%9A%84%E6%8A%98%E6%89%A3%E4%BD%BF%E7%94%A8mapvalues%E4%BF%AE%E6%94%B9value)
      - [(5) Use Case 3：为购买5本及以上字典的顾客赠送牙刷（使用`flatMapValue`为指定的key增加value）](#5-use-case-3%E4%B8%BA%E8%B4%AD%E4%B9%B05%E6%9C%AC%E5%8F%8A%E4%BB%A5%E4%B8%8A%E5%AD%97%E5%85%B8%E7%9A%84%E9%A1%BE%E5%AE%A2%E8%B5%A0%E9%80%81%E7%89%99%E5%88%B7%E4%BD%BF%E7%94%A8flatmapvalue%E4%B8%BA%E6%8C%87%E5%AE%9A%E7%9A%84key%E5%A2%9E%E5%8A%A0value)
      - [(6) Use Case 4：为消费最高的客户赠送睡衣（用`RDD.reduceByKey/foldByKey`合并每个key的所有value）](#6-use-case-4%E4%B8%BA%E6%B6%88%E8%B4%B9%E6%9C%80%E9%AB%98%E7%9A%84%E5%AE%A2%E6%88%B7%E8%B5%A0%E9%80%81%E7%9D%A1%E8%A1%A3%E7%94%A8rddreducebykeyfoldbykey%E5%90%88%E5%B9%B6%E6%AF%8F%E4%B8%AAkey%E7%9A%84%E6%89%80%E6%9C%89value)
      - [(7) Use Case 5：列出每个顾客购买的所有产品（使用`RDD.aggregateByKey`对每个Key的所有Value做转换再合并）](#7-use-case-5%E5%88%97%E5%87%BA%E6%AF%8F%E4%B8%AA%E9%A1%BE%E5%AE%A2%E8%B4%AD%E4%B9%B0%E7%9A%84%E6%89%80%E6%9C%89%E4%BA%A7%E5%93%81%E4%BD%BF%E7%94%A8rddaggregatebykey%E5%AF%B9%E6%AF%8F%E4%B8%AAkey%E7%9A%84%E6%89%80%E6%9C%89value%E5%81%9A%E8%BD%AC%E6%8D%A2%E5%86%8D%E5%90%88%E5%B9%B6)
  - [4.2 理解数据分区并减少数据Shuffling](#42-%E7%90%86%E8%A7%A3%E6%95%B0%E6%8D%AE%E5%88%86%E5%8C%BA%E5%B9%B6%E5%87%8F%E5%B0%91%E6%95%B0%E6%8D%AEshuffling)
    - [4.2.1 使用Spark的Data Partitioners](#421-%E4%BD%BF%E7%94%A8spark%E7%9A%84data-partitioners)
      - [(1) Spark提供的Partitioner](#1-spark%E6%8F%90%E4%BE%9B%E7%9A%84partitioner)
      - [(2) 自定义Pair RDD Partitioner](#2-%E8%87%AA%E5%AE%9A%E4%B9%89pair-rdd-partitioner)
    - [4.2.2 理解和避免不必要的Data Shuffling](#422-%E7%90%86%E8%A7%A3%E5%92%8C%E9%81%BF%E5%85%8D%E4%B8%8D%E5%BF%85%E8%A6%81%E7%9A%84data-shuffling)
      - [(1) 产生shuffle的情况](#1-%E4%BA%A7%E7%94%9Fshuffle%E7%9A%84%E6%83%85%E5%86%B5)
        - [(a) 显式地更换了Partitioner](#a-%E6%98%BE%E5%BC%8F%E5%9C%B0%E6%9B%B4%E6%8D%A2%E4%BA%86partitioner)
        - [(b) Partitioner被`map`和`flatMap`删除导致shuffle](#b-partitioner%E8%A2%ABmap%E5%92%8Cflatmap%E5%88%A0%E9%99%A4%E5%AF%BC%E8%87%B4shuffle)
      - [(2) 使用External Shuffle Service优化Shuffling](#2-%E4%BD%BF%E7%94%A8external-shuffle-service%E4%BC%98%E5%8C%96shuffling)
      - [(3) 影响shuffling的参数](#3-%E5%BD%B1%E5%93%8Dshuffling%E7%9A%84%E5%8F%82%E6%95%B0)
    - [4.2.3. 显式地对RDDs进行重新分区](#423-%E6%98%BE%E5%BC%8F%E5%9C%B0%E5%AF%B9rdds%E8%BF%9B%E8%A1%8C%E9%87%8D%E6%96%B0%E5%88%86%E5%8C%BA)
      - [(1) `RDD.PartitionBy`](#1-rddpartitionby)
      - [(2) `RDD.coalesce` 和 `RDD.repartition`](#2-rddcoalesce-%E5%92%8C-rddrepartition)
      - [(3) `RDD.repartitionAndSortWithinPartition`](#3-rddrepartitionandsortwithinpartition)
    - [2.4.2 在每个Partition上单独执行Mapping操作](#242-%E5%9C%A8%E6%AF%8F%E4%B8%AApartition%E4%B8%8A%E5%8D%95%E7%8B%AC%E6%89%A7%E8%A1%8Cmapping%E6%93%8D%E4%BD%9C)
      - [(1) `mapPartitions` 和 `mapPartitionsWithIndex`](#1-mappartitions-%E5%92%8C-mappartitionswithindex)
      - [(2) 使用`glom`收集分区数据](#2-%E4%BD%BF%E7%94%A8glom%E6%94%B6%E9%9B%86%E5%88%86%E5%8C%BA%E6%95%B0%E6%8D%AE)
  - [4.3 Joining、sorting以及grouping](#43-joiningsorting%E4%BB%A5%E5%8F%8Agrouping)
    - [4.3.1 Joining](#431-joining)
      - [(1) `join`/`leftOuterJoin`/`rightOuterJoin`/`fullOuterJoin`](#1-joinleftouterjoinrightouterjoinfullouterjoin)
      - [(2) 使用`substract`和`subtractByKey` 求差集](#2-%E4%BD%BF%E7%94%A8substract%E5%92%8Csubtractbykey-%E6%B1%82%E5%B7%AE%E9%9B%86)
      - [(3) `cogroup` ：返回每个key在两个RDD中的迭代器](#3-cogroup-%E8%BF%94%E5%9B%9E%E6%AF%8F%E4%B8%AAkey%E5%9C%A8%E4%B8%A4%E4%B8%AArdd%E4%B8%AD%E7%9A%84%E8%BF%AD%E4%BB%A3%E5%99%A8)
      - [(3) `intersection`：求RDD交集](#3-intersection%E6%B1%82rdd%E4%BA%A4%E9%9B%86)
      - [(4) `cartesian` ：求两个RDD的笛卡尔积](#4-cartesian-%E6%B1%82%E4%B8%A4%E4%B8%AArdd%E7%9A%84%E7%AC%9B%E5%8D%A1%E5%B0%94%E7%A7%AF)
      - [(5) `zip` ：Joining RDDs](#5-zip-joining-rdds)
      - [(6) `zipPartitions`](#6-zippartitions)
    - [4.3.2 排序（soriting）](#432-%E6%8E%92%E5%BA%8Fsoriting)
      - [(1) `sortBy`](#1-sortby)
      - [(2) `sortByKey`](#2-sortbykey)
        - [(a) 方法签名](#a-%E6%96%B9%E6%B3%95%E7%AD%BE%E5%90%8D)
        - [(b) 使用](#b-%E4%BD%BF%E7%94%A8)
        - [(c) 让key变成ordable key](#c-%E8%AE%A9key%E5%8F%98%E6%88%90ordable-key)
          - [方法1：使用`Ordered Trait`](#%E6%96%B9%E6%B3%951%E4%BD%BF%E7%94%A8ordered-trait)
          - [方法2：使用`Ordering Trait`](#%E6%96%B9%E6%B3%952%E4%BD%BF%E7%94%A8ordering-trait)
      - [(3) 使用`groupByKeyAndSortValues` 按照key分组并对value进行二级排序](#3-%E4%BD%BF%E7%94%A8groupbykeyandsortvalues-%E6%8C%89%E7%85%A7key%E5%88%86%E7%BB%84%E5%B9%B6%E5%AF%B9value%E8%BF%9B%E8%A1%8C%E4%BA%8C%E7%BA%A7%E6%8E%92%E5%BA%8F)
      - [(4) 使用 `top` 和 `takeOrdered` 来读取排序后的元素](#4-%E4%BD%BF%E7%94%A8-top-%E5%92%8C-takeordered-%E6%9D%A5%E8%AF%BB%E5%8F%96%E6%8E%92%E5%BA%8F%E5%90%8E%E7%9A%84%E5%85%83%E7%B4%A0)
    - [4.3.3 数据分组（Grouping）](#433-%E6%95%B0%E6%8D%AE%E5%88%86%E7%BB%84grouping)
      - [(1) `groupByKey` 和 `groupBy`](#1-groupbykey-%E5%92%8C-groupby)
      - [(2) `combineByKey`](#2-combinebykey)
  - [4.4 理解RDD依赖](#44-%E7%90%86%E8%A7%A3rdd%E4%BE%9D%E8%B5%96)
    - [4.4.1. RDD依赖和Spark Job执行](#441-rdd%E4%BE%9D%E8%B5%96%E5%92%8Cspark-job%E6%89%A7%E8%A1%8C)
      - [执行模式](#%E6%89%A7%E8%A1%8C%E6%A8%A1%E5%BC%8F)
    - [4.2.2 Spark Job执行](#422-spark-job%E6%89%A7%E8%A1%8C)
    - [4.3.3 使用`Check Point`保存`RDD Lineage`](#433-%E4%BD%BF%E7%94%A8check-point%E4%BF%9D%E5%AD%98rdd-lineage)
  - [4.5 `Accumulators` 以及`broadcast variables`](#45-accumulators-%E4%BB%A5%E5%8F%8Abroadcast-variables)
    - [4.5.1 使用Accumulator从Executor获取数据](#451-%E4%BD%BF%E7%94%A8accumulator%E4%BB%8Eexecutor%E8%8E%B7%E5%8F%96%E6%95%B0%E6%8D%AE)
      - [(1) Accumulator](#1-accumulator)
      - [(2) 编写自定义Accumulator（使用`AccumulatorParam`和`Accumulable`）](#2-%E7%BC%96%E5%86%99%E8%87%AA%E5%AE%9A%E4%B9%89accumulator%E4%BD%BF%E7%94%A8accumulatorparam%E5%92%8Caccumulable)
      - [(3) 使用Accumulable Collection来存储全局累加值](#3-%E4%BD%BF%E7%94%A8accumulable-collection%E6%9D%A5%E5%AD%98%E5%82%A8%E5%85%A8%E5%B1%80%E7%B4%AF%E5%8A%A0%E5%80%BC)
    - [4.5.2 Broadcast Variables](#452-broadcast-variables)
      - [配置参数](#%E9%85%8D%E7%BD%AE%E5%8F%82%E6%95%B0)
  - [4.6 Summary](#46-summary)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CH04 Spark API深入

代码

> scala：[../ch04/scala/ch04-listings.scala](../ch04/scala/ch04-listings.scala)
>
> python: [../ch04/python/ch04-listings.py](../ch04/python/ch04-listings.py)
>
> java: [../ch04/java/](../ch04/java/)

## 4.1 `Pair RDDs`

> `pair RDDS`：包含key-value二元组的RDD

### 4.1.1 创建`Pair RDD`

方法 1：`SparkContext`一些方法默认创建Pair RDD（例如：读取HDFS文件创建的RDD）

方法 2：使用`keyBy` transformation，该方法接受一个用于生成key的函数 

方法 3：使用类似下面的代码，将其转换为Pair RD

> ~~~scala
> var transByCust = tranData.map(tran => (tran(2).toInt /*key*/, tran/*value*/))
> ~~~

说明

> 1. 定义在`PairRDDFunctions`中的方法，会通过implicit conversion自动可用（详见CH02）
> 2. Java代码中创建Pair RDDS的方法：[creating pair RDDS in Java](https://livebook.manning.com/book/spark-in-action/chapter-4/12)

### 4.1.2 `Pair RDD`函数

#### (1) 背景

##### (a) Use Case

> Use Case 1：赠品`玩具熊`，为交易量最大的客户
>
> Use Case 2：奖励`5%折扣`，为购买"Barbie Shopping Mall Playsets"两套及以上的客户
>
> Use Case 3：赠品`牙刷`，为购买字典5本及以上的客户
>
> Use Case 4：赠品`睡衣`，为消费最高的客户
>
> Use Case 5：列出每个顾客购买的所有产品

##### (b) 数据

> 每行一笔交易；字段包括：`date`、 `time`、 `customer ID`、`product ID`、`quantity`、`product price`；用"#"分隔

#### (2) 创建`Pair RDD`

使用上一节的`.map(...)`方法将RDD转换成`Pair RDD`

> ~~~scala
> val tranFile = sc.textFile("first-edition/ch04/ch04_data_transactions.txt")
> val tranData = tranFile.map(_.split("#")) // 第2列是customer id
> // 用map方法生成Pair RDD，key是customer id
> var transByCust = tranData.map(tran => (tran(2).toInt, tran)) 
> 
> transByCust.keys.distinct().count()
> // res0: Long = 100
> ~~~

如果想获得`Pair RDD`的key和value

> ~~~scala
> PairRDDFunctions.keys()   // 等价于map(_._1)
> PairRDDFunctions.values() // 等价于map(_._2)
> ~~~

#### (3) Use Case 1：为交易量最大的客户赠送玩具熊（找到value数量最大的key）

计算每个顾客的交易数量（每个key的value数量）

> ~~~scala
> // 与`RDD transformation`不同、`RDD action`返回Java/Scala/Python对象，而非另一个RDD
> transByCust.countByKey()
> // res1: scala.collection.Map[Int,Long] = Map(69 -> 7, 88 -> 5, 5 -> 11, 10 -> 7, 56 -> 17, 42 -> 7, 24 -> 9, 37 -> 7, 25 -> 12, 52 -> 9, 14 -> 8, 20 -> 8, 46 -> 9, 93 -> 12, 57 -> 8, 78 -> 11, 29 -> 9, 84 -> 9, 61 -> 8, 89 -> 9, 1 -> 9, 74 -> 11, 6 -> 7, 60 -> 4,...
> 
> // 下面的sum是scala的方法、不是Spark的
> transByCust.countByKey().values.sum
> // res3: Long = 1000
> ~~~

为交易数量最多的顾客（value数量最多的key）赠送玩具熊（product id：4）

> ~~~scala
> // 交易数量最高的顾客
> val (cid, purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
> // cid: Int = 53
> // purch: Long = 19
> 
> // 将玩具熊添加到赠品数组中（在Use Case 4中会统一处理）
> var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))
> ~~~

查看该顾客的所有交易（查看key的所有value）

> ~~~scala
> // 该顾客所有的交易
> transByCust.lookup(53)
> // res1: Seq[Array[String]] = WrappedArray(Array(2015-03-30, 6:18 AM, 53, 42, 5, 2197.85), Array(2015-03-30, 4:42 AM, 53, 3, 6, 9182.08), ...
> // WrappedArray是scala通过implicit conversion将Array表示成Seq（mutable sequence）的方法
> 
> transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))
> // 2015-03-30, 6:18 AM, 53, 42, 5, 2197.85
> // 2015-03-30, 4:42 AM, 53, 3, 6, 9182.08
> // ...
> ~~~

#### (4) Use Case 2：为满足条件的客户提供5%的折扣（使用`mapValues`修改value）

> ~~~scala
> transByCust = transByCust.mapValues(tran => {
> 	// 对于购买Barbie Shopping Mall Playsets（product_id:25）两套及以上的顾客
> 	if(tran(3).toInt == 25 && tran(4).toDouble > 1)
> 		// 赠送5%的折扣
> 		tran(5) = (tran(5).toDouble * 0.95).toString
> 	tran })
> ~~~

#### (5) Use Case 3：为购买5本及以上字典的顾客赠送牙刷（使用`flatMapValue`为指定的key增加value）

> ~~~scala
> // 每个Value是flatMap concanated the WrapArrays, 
> // thus each element is a inner array which represent a transaction
> transByCust = transByCust.flatMapValues(tran => {
> 	// 3:product ID; 81: dictionary; 4; quantity; 5:product price
> 	// toothbrush (ID 70) , dictionaries (ID 81)
> 	if(tran(3).toInt == 81 && tran(4).toInt >= 5) {
> 		val cloned = tran.clone()
> 		cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
> 		List(tran, cloned)
> 	} else {
> 		List(tran)
>     }
> })
> ~~~
>
> 如Use Case 1的代码，每个key有多个value，这些Value被保存在一个WrappedArray中
>
> ~~~scala
> // 该顾客所有的交易
> transByCust.lookup(53)
> // res1: Seq[Array[String]] = WrappedArray(
> //	Array(2015-03-30, 6:18 AM, 53, 42, 5, 2197.85), 
> //	Array(2015-03-30, 4:42 AM, 53, 3, 6, 9182.08), 
> //	...
> // )
> ~~~
>
> flatMap方法将WrappedArray展开，这样它传给`函数`的是每一个transaction，而不是一个WrappedArray

#### (6) Use Case 4：为消费最高的客户赠送睡衣（用`RDD.reduceByKey/foldByKey`合并每个key的所有value）

功能：将同一个key的所有value合并在在一个value中

> * `RDD.foldByKey`需要提供一个参数`zeroValue`，`RDD.reduceByKey`不需要
> * `zeroValue`需要是一个自然值（例如加法中的0、乘法中的1、列表中的Nil、……），使用时要考虑RDD的并行特点

方法签名

> ~~~scala
> foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
> reduceByKey(func: (V, V) => V): RDD[(K, V)]
> ~~~

代码

> ~~~scala
> // 找到消费最高的顾客
> // * `RDD.reduceByKey` operation merge all the values of a key into a single value of the same type, according to the `merge` function passed in
> // * `RDD.foldByeKey'  operation does the same thing, except that it requires an additional parameter `zeroValue`
> // * `zeroValue` should be a neutral value (0 for addition, 1 for multiplication, Nil for lists, and so forth)
> val amounts = transByCust.mapValues(t => t(5).toDouble)
> val totals  = amounts.foldByKey(0)((p1, p2) => p1 + p2).collect()
> // res0: Array[(String, Double)] = Array((84,53020.619999999995), (96,36928.57), (66,52130.01), (54,36307.04), 
> totals.toSeq.sortBy(_._2).last
> // res1: (Int, Double) = (76,100049.0)
> 
> // 参数zeroValue会因为RDD的parallel特点，被使用多次
> // 如果它代表的不是零值、在foldByKey过程中会被累加多次
> // * `zeroValue` should be a neutral value (0 for addition, 1 for multiplication, Nil for lists, and so forth), 
> // * it applied on the first value of a key and might be applied multiple times due to the RDD's parallel nature.
> // * demonstrate that `zerovalue` parameter will be added more than once during the computation
> amounts.foldByKey(100000)((p1, p2) => p1 + p2).collect()
> 
> // 将给该顾客（ID 76）的睡衣（ID：63）添加到赠品数组中
> complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")
> 
> // 将赠品数组Union到Customer Transaction RDD中，并存储在文件中
> // 其中
> // * sc.parallelize(Seq(Array))将本地scala数据结构转换成RDD
> // * t(2): column 2 is customer ID
> transByCust = transByCust.union(sc.parallelize(complTrans).map(t => (t(2).toInt, t)))
> transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-transByCust")
> ~~~

#### (7) Use Case 5：列出每个顾客购买的所有产品（使用`RDD.aggregateByKey`对每个Key的所有Value做转换再合并）

`RDD.aggregateByKey`与之前的`RDD.foldByKey/reduceByKey`的差别是

> * `RDD.foldByKey/reduceByKey`：不对value类型做转换，直接合并成同类型的value（`(V, V) => V`）
> * `RDD.aggregateByKey`：先对类型做转换（`(U, V) => U`）再进行合并（`(U, U) => U)`）

方法签名

> ~~~scala
> // Aggregate the values of each key, using given combine functions and a neutral "zero value".
> def aggregateByKey[U]
> 	(zeroValue: U)
> 	(seqOp: (U, V) => U, combOp: (U, U) => U)
> 	(implicit arg0: ClassTag[U])
> : RDD[(K, U)]
> ~~~

参数包括

> `zeroValue` ：输出类型U的零值
>
> `seqOp`：转换函数，遇到一个输入类型`V`时，将它转换并合并到一个输出类型`U`上
>
> `combOp`：合并函数，合并两个输出类型`U`

备注

> 使用两个参数列表（`zeroValue: U`和`seqOp: (U, V) => U, combOp: (U, U) => U`）被称为咖喱语法（currying），可参考 [www.scala-lang.org/old/node/135](www.scala-lang.org/old/node/135)

代码：列出每个用户购买的所有产品

> ~~~scala
> // trans(3) is customer_id
> // zeroValue List[String](), an empty string
> // seqOp (U, V) => (U): (prods, tran) => prods ::: List(tran(3)) 
> // combOp (U, U) => (U): (prods1, prods2) => prods1 :: prods2
> val prods = transByCust.aggregateByKey(List[String]())(
>    (prods, tran) => prods ::: List(tran(3)),
>    (prods1, prods2) => prods1 ::: prods2)
> prods.collect()
> // res0: Array[(String, List[String])] = Array((88,List(47.149.147.123, 74.211.5.196,...), (82,List(8.140.151.84, 23.130.185.187,...), ...)
> ~~~
>
> 其中的 `:::`用来拼接两个list

## 4.2 理解数据分区并减少数据Shuffling

> Spark以分布式分片的方式存放计算数据，每个数据分片被称为一个partition（曾经被称为splits）。每个RDD维护一个Partition列表，以及一个可选的用于计算partation存放位置的prefer locations列表。访问`RDD.partitions`字段可以得到这个RDD的分区列表

### 4.2.1 使用Spark的Data Partitioners

#### (1) Spark提供的Partitioner

> Spark提供 `HashPartioner`和 `RangePartitioner`两种方式来计算分区
>
> * `HashPartitioner`：是默认的partationer，分区数量配置在`spark.default.parallelism`配置项中，第12章有详细参考
> * `RangePartition`：根据通过采样，将数据根据key的取值范围将数据分区到大致跨度相等的分区中

#### (2) 自定义Pair RDD Partitioner

用途

> 有时需要自定义RDD Partitioner，例如”让Spark Job的分区与某个数据表的分区一致，每个子任务只处理一个表分区的数据

使用方法

> 除了mapValues和flatMapValues以外，所以的RDD trasformations都提供了一个重载版本、可以传入自定义Partitioner
>
> ~~~scala
> // 使用父RDD的Partitioner，如果没有就使用默认的HashPartitioner(spark.default.parallelism)
> rdd.foldByKey(afunction) 
> 
> // 使用new HashPartitioner(100)作为Partitioner
> rdd.foldByKey(afunction, 100)
> 
> // 使用new HashPartitioner(100)作为Partitioner
> rdd.foldByKey(afunction, new HashPartitioner(100))
> ~~~
>
> 另外还可以通过修改key的hash code来影响HashPartitioner对这个RDD的分区

### 4.2.2 理解和避免不必要的Data Shuffling

#### (1) 产生shuffle的情况

##### (a) 显式地更换了Partitioner

> 1. `Pair RDD transformation`中使用了自定义Partition会导致shuffling
> 2. 使用的`HashPartitioner`与父RDD不同时会导致shuffler
>
> 例如
>
> ~~~scala
> // assume RDD's parallelism is different than 100
> rdd.aggregateByKey(zeroValue, 100)(
> 	seqFunc, comboFunc
> ).collect() 
> rdd.aggregateByKey(zeroValue, new CustomPartitioner())(
> 	seqFunc, comboFunc
> ).collect()
> ~~~
>
> 因此编写代码时，尽量使用默认默认Partitioner以避免此类shuffler

##### (b) Partitioner被`map`和`flatMap`删除导致shuffle

> `map` 和 `flatMap` 会删除当前的Partitioner，有可能会导致后续的transformation发生shuffling，例如下面例子中的 `reduceByKey` 
>
> ~~~scala
> import org.apache.spark.rdd.RDD
> // 普通RDD
> val rdd:RDD[Int] = sc.parallelize(1 to 10000)
> 
> // 不会引发shuffling
> rdd.map(x => (x, x*x)) 	// 创建Pair RDD
> 	.map(_.swap)	   	// 交换key和value
> 	.collect()
> 
> // 引发shuffling
> rdd.map(x => (x, x*x))	
> 	.reduceByKey((v1, v2)=>v1+v2) // 发生shuffling
> 	.collect()
> ~~~
>
> `map`和`flatMap`会导致如下的后续transformations发生shuffling
>
> * Pair RDD transformations：`aggregateByKey`, `foldByKey`, `reduceByKey`, `groupByKey`, `join`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin`,  `subtractByKey`
> * RDD transformations：`subtract`, `intersection`, and `groupWith``
> * `sortByKey` transformation 总是会产生shuffle
> * 带有`shuffle = true`的`partitionBy` 或 `coalesce`  transformation

#### (2) 使用External Shuffle Service优化Shuffling

> 某些executors被kill，会导致其他的executors无法获得shuffle数据、进而中断整个data flow。开启external shuffle server可以解决此问题。配置项是`spark.shuffle.service.enabled`。它会为每一个work node开启一个external shuffle server

#### (3) 影响shuffling的参数

`spark.shuffle.manager parameter`: `hash` or `sort`

> 设置shuffle的具体实现，参考链接：[http://mng.bz/s6EA](http://mng.bz/s6EA)

`spark.shuffle.consolidateFiles`：`true` or `false`（默认值为false）

> 用来决定是否consolidate中间文件，如果使用的是ext4或者XFS文件系统，推荐将此配置设为true。更多内容参考 [http://http://mng.bz/O304](http://mng.bz/O304)

`spark.shuffle.spill`：`true` or `false` （默认值为true）

> 指定是否限制任务内存，超出的数据会“spill over to disk”

`spark.shuffle.memoryFraction`:

> 设置内存限制，太高容易产生OOM，太低容易引发频繁的spilling

`spark.shuffle.spill.compress`: `true` or `false` （默认值为true）

> 设置是否压缩中间文件

`spark.shuffle.spill.batchSize`（默认值10000）

>  发生"spilling"时、一起序列化到磁盘中的对象数量

`spark.shuffle.service.port`

> `external shuffle service`的端口

### 4.2.3. 显式地对RDDs进行重新分区

> 当父RDD的分区不适合时（例如分区数太小，并行度不够），可以通过以下方法显式地进行重新分区
>
>  `RDD.partitionBy`, `RDD.coalesce`, `repartition`, `repartitionAndSortWithinPartition`

#### (1) `RDD.PartitionBy`

> 仅适用于`Pair RDDs`，只支持传入一个参数，即Partitioner对象

#### (2) `RDD.coalesce` 和 `RDD.repartition`

>  `coalesce` 用于增加或减少分区数量，方法签名如下、第二个参数用来指定是否需要shuffle
>
> ~~~scala
> coalesce (numPartitions: Int, shuffle: Boolean = false)
> ~~~
>
> `repartitioning`：平衡partition之间的数据，在不改变partition数量尽量维持locality的前提下、在机器之间移动数据

#### (3) `RDD.repartitionAndSortWithinPartition`

> 仅适用于sortable RDD（带有sortable key的Pair RDD），它根据传入的Partitioner进行重新分区，并且在每个分区中根据key对数据进行排序
>
> 方法签名如下
>
> ~~~scala
> repartitionAndSortWithinPartitions(
> 	numPartitions=None, 
>     partitionFunc=<function portable_hash>, 
>     ascending=True, 
>     keyfunc=<function RDD.<lambda>>)[source]
> ~~~

### 2.4.2 在每个Partition上单独执行Mapping操作

> 使用 `mapPartitions`, `mapPartitionsWithIndex`, `glom`等API，可以让mapping操作在每个Partition上单独执行，从而避免了shuffling

#### (1) `mapPartitions` 和 `mapPartitionsWithIndex`

方法签名如下

`mapPartitions`

> ~~~scala
> // Return a new RDD by applying a function to each partition of this RDD
> def mapPartitions[U](
>     f: (Iterator[T]) => Iterator[U], 		// mapping function，Iterator[T]可以用来遍历当前partition的元素
>     preservesPartitioning: Boolean = false	// 
> ) (implicit arg0: ClassTag[U])
> : RDD[U]
> ~~~

`mapPartitionsWithIndex`

> ~~~scala
> // Return a new RDD by applying a function to each partition of this RDD, 
> // while tracking the index of the original partition.
> def mapPartitionsWithIndex[U](
> 	// mapping function，
> 	// * Iterator[T]可以用来遍历当前partition的元素
> 	// * Int可以用来跟踪是original partition中的哪个
> 	f: (Int, Iterator[T]) => Iterator[U], 
> 	preservesPartitioning: Boolean = false
> )(implicit arg0: ClassTag[U])
> : RDD[U]
> ~~~

这样两个方法在如下场景尤其有用：

> 需要访问外部数据库时，使用mapPartition或mapPartitionWithIndex只需要为每个分区创建一个数据库连接，而不用为每个元素创建一次DB连接

更多参考: [http://mng.bz/CWA6](http://mng.bz/CWA6)

#### (2) 使用`glom`收集分区数据

> `glom` （means grab）将每个partition的数据收集到一个Array中，然后返回一个封装了这些Array的RDD。例子如下
>
> ~~~scala
> scala> val list = List.fill(500)(scala.util.Random.nextInt(100))
> list: List[Int] = List(88, 59, 78, 94, 34, 47, 49, 31, 84, 47, ...)
> scala> val rdd = sc.parallelize(list, 30).glom()
> rdd: org.apache.spark.rdd.RDD[Array[Int]] = MapPartitionsRDD[0]
> scala> rdd.collect()
> res0: Array[Array[Int]] = Array(Array(88, 59, 78, 94,...), ...)
> scala> rdd.count()
> res1: Long = 30
> ~~~

## 4.3 Joining、sorting以及grouping

Use Case：

> 1. 商品名称，总售卖量，按照字母顺序排序
> 2. 商品列表，包含前一天没有卖出的商品
> 3. 前一天顾客交易的统计信息，包括每个顾客的：average、maximum,、minimum、total price

数据：加载文件得到的`tranData RDD` 

> 字段包括`date`、 `time`、 `customerID`、 `productID`、 `quantity`、 `productPrice`，使用“#”分隔

### 4.3.1 Joining

要Join的Pair RDD

> ~~~scala
> // 商品交易: prod_id -> transaction
> // tran(3)是product id
> val transByProd = tranData
> 	.map(tran => (tran(3).toInt, tran))
> 
> // 商品销量: prod_id -> count
> // t(5)是销量quantity
> val totalsByProd = transByProd
> 	.mapValues(t => t(5).toDouble)
> 	.reduceByKey{case(tot1, tot2) => tot1 + tot2}
> 
> // 商品信息: prod_id -> product
> // p(0)是product id
> val products = sc.textFile("first-edition/ch04/ch04_data_products.txt")
> 	.map(line => line.split("#"))
> 	.map(p => (p(0).toInt, p))
> ~~~

可以使用`joins`、`zip`、`cartesian`、`intersection`等transformation来Join这些数据

#### (1) `join`/`leftOuterJoin`/`rightOuterJoin`/`fullOuterJoin`

> 这些transformation的行为与RMDB Table Join相似
>
> * `join`：返回`Pair RDD：(K, (V,W))`, 其中 `(V,W)`包含同一个Key来自两个RDD的所有value组合
> * `leftOuterJoin`: 返回 `Pair RDD: (K, (V, Option(W))`， 在join的基础上为只在第1个RDD中有数据的key增加了 `(key, (v, None))`元素
> * `rightOuterJoin`: 返回 `pair RDD：(K, (Option(V), W)`，在join的基础上为只在第2个RDD中有数据的key增加了 `(key, (None, W))`元素
> * `fullOuterJoin`: 返回 `pair RDD：(K, (Option(V), Option(W))`, 在join的基础上为只在任意1个RDD中有数据的key增加了 `(key, (v, None))` 或者 `(key, (None, w))` 元素

Scala的[Option](http://www.scala-lang.org/api/2.13.1/scala/Option.html)类：避免NullPointerException，封装None或者某个对象

> `Option.isEmpty`: 检查是否有值
>
> `Option.get`: 有值会返回封装在其中的对象、没有值会返回None
>
> `Option.getOrElse(default)`: 有值返回封装在其中的对象，没有值返回从参数default传入的默认值

说明

> 1. 如果某个RDD中某个key出现多次，那么在join时也会参与多次
>
> 2. Partitioner选择
>
>     1st priority：执行transformation时从参数传入的 `Partitioner`
>
>     2st priority：Left RDD的Partition
>
>     3rd priority：Right RDD的Partition
>
>     4th priority：使用 `spark.default.partitions`来new一个新的 `HashPartitioner`

代码

> ~~~scala
> // 计算每个有销量的product的销量
> // 输入
> // * totalsByProd : prod_id -> prod_count
> // * products: prod_id -> prod_info
> // 输出 
> // * prod_id -> (prod_count, prod_info)
> val totalsAndProds = totalsByProd.join(products)
> totalsAndProds.first()
> // res0: (Int, (Double, Array[String])) = (84,(75192.53,Array(84, Cyanocobalamin, 2044.61, 8)))
> 
> // 计算公司没有卖出的product
> val totalsWithMissingProds = totalsByProd.rightOuterJoin(products)
> // 输出格式：(Key, (Product, Option(Int)))
> val missingProds = totalsWithMissingProds
> 	.filter(x => x._2._1 == None) //._2, ._1 是元组选择语法
> 	.map(x => x._2._2)
> missingProds.foreach(p => println(p.mkString(", ")))
> // 43, Tomb Raider PC, 2718.14, 1
> // 63, Pajamas, 8131.85, 3
> // 3, Cute baby doll, battery, 1808.79, 2
> // 20, LEGO Elves, 4589.79, 4
> ~~~

#### (2) 使用`substract`和`subtractByKey` 求差集

> `substract`: 返回一个PairRDD，存储着在第1个RDD中存在但是在第2个RDD中不存在的元素
>
> `substractByKey`: 返回一个 Pair RDD，存储着在第1个RDD中存在但是在第2个RDD中不存在的key所对应的元素
>
> ~~~scala
> val missingProds = products.subtractByKey(totalsByProd).values
> missingProds.foreach(p => println(p.mkString(", ")))
> // 20, LEGO Elves, 4589.79, 4
> // 3, Cute baby doll, battery, 1808.79, 2
> // 43, Tomb Raider PC, 2718.14, 1
> // 63, Pajamas, 8131.85, 3
> ~~~

#### (3) `cogroup` ：返回每个key在两个RDD中的迭代器

> cogroup transformation按照key聚合多个RDD中的values，返回一个存放迭代器数组的Pair RDD、其中：
>
> * 每个key一个迭代器数组
> * 数组中每个迭代器、用于访问一个Input RDD中这个key对应的value

方法签名

> ~~~scala
> cogroup[W1, W2](
>     other1: RDD[(K, W1)], other2: RDD[(K, W2)]
> ): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
> ~~~

代码

> ~~~scala
> // totalsByProd  : prod_id -> prod_count
> // products      : prod_id -> prod
> // prodTotCogroup: prod_id -> (Iterable(prod_count), Iterable(prod))
> val prodTotCogroup = totalsByProd.cogroup(products)
> // x._2._1: Iterable(prod_count) 
> // x._2._2: Iterable(prod)
> prodTotCogroup
> 	// 只保留在totalsByProd中不存在的key
> 	.filter(x => x._2._1.isEmpty) 
> 	// 查看每个key的第一个prod
> 	.foreach(x => println(x._2._2.head.mkString(", "))) 
> // 43, Tomb Raider PC, 2718.14, 1
> // 63, Pajamas, 8131.85, 3
> // 3, Cute baby doll, battery, 1808.79, 2
> // 20, LEGO Elves, 4589.79, 4
> 
> val totalsAndProds = prodTotCogroup
> 	// 只保留在totalsByProd中存在的key
> 	.filter(x => !x._2._1.isEmpty) 
> 	// prod_id, prod<prod_id>[0].prod_count, prod<prod_id>[0].prod
> 	.map(x => (x._2._2.head(0).toInt, (x._2._1.head, x._2._2.head)))
> ~~~
>
> `x._2._1` selects the iterator with matching values from totalsByProd (totals as Doubles)
> `x._2._2` selects the iterator with products as Arrays of Strings
> `x._2._2.head` 1st element of Iterable(prod) 
> `x._2._2.head(0)` product_id (1st column) of 1st element(.head) of Iterable(prod)

#### (3) `intersection`：求RDD交集

> 返回一个新的Pair RDD，包含在两个RDD中都存在的元素
>
> ~~~scala
> // totalsByProd: prod_id -> prod_count
> // return: intersection of prod_id of totalsByProd and products
> totalsByProd.map(_._1).intersection(products.map(_._1))
> ~~~

#### (4) `cartesian` ：求两个RDD的笛卡尔积

方法签名

> ~~~scala
> // Return the Cartesian product of this RDD and another one, 
> // that is, the RDD of all pairs of elements (a, b) 
> // where a is in this and b is in other.
> def cartesian[U](
> 	other: RDD[U]
> ) (implicit arg0: ClassTag[U]): RDD[(T, U)]
> ~~~

代码

> ~~~scala
> val rdd1 = sc.parallelize(List(7,8,9))
> val rdd2 = sc.parallelize(List(1,2,3))
> rdd1.cartesian(rdd2).collect()
> // res0: Array[(Int, Int)] = Array((7,1), (7,2), (7,3), (8,1), 
> // (9,1), (8,2), (8,3), (9,2), (9,3))
> 
> rdd1.cartesian(rdd2)
> 	.filter(el => el._1 % el._2 == 0)
> 	.collect()
> // res1: Array[(Int, Int)] = Array((7,1), (8,1), (9,1), (8,2), (9,3))
> ~~~

需要强调的是求笛卡尔积，要在所有分区之间传递数据，产生大量的二元组，消耗大量内存

#### (5) `zip` ：Joining RDDs

> 像拉衣服拉链一样，将两个RDD位置相同的元素、两两组装成二元组
>
> 要求两个RDD的分区数、元素数量相同，否则会运行出错
>
> 当一个RDD是由另一个RDD使用map方法计算得来时，使用zip方法会很安全

例子

> ~~~scala
> val rdd1 = sc.parallelize(List(1,2,3))
> val rdd2 = sc.parallelize(List("n4","n5","n6"))
> rdd1.zip(rdd2).collect()
> // res1: Array[(Int, Int)] = Array((1,"n4"), (2,"n5"), (3,"n6"))
> ~~~

#### (6) `zipPartitions` 

> 将两个RDD的分区（而不是元素）zip起来，只要求分区数相同
>
> 签名
>
> ~~~scala
> // zip this RDD's partitions with one (or more) RDD(s) and return a new RDD 
> // by applying a function to the zipped partitions
> def zipPartitions[B, V](
> 	rdd2: RDD[B],                  // 要与当前RDD进行zip partition的RDD
> 	preservesPartitioning: Boolean // 为true时会关闭shuffling
> )(
> 	// 输入：用来遍历Input Parttion中Element的迭代器
> 	// 输出：用来遍历Output Partition中Element的迭代器
> 	// Input Partition和Output Partition中Element的数量不一定相同
> 	f: (Iterator[T], Iterator[B]) => Iterator[V]
> )(implicit arg0: ClassTag[B], arg1: ClassTag[V])
> : RDD[V] // 返回一个Partition
> ~~~
>
> 例子
>
> ~~~scala
> val rdd1 = sc.parallelize(1 to 10, 10)                 // 10个整除，存储在10个分区中
> val rdd2 = sc.parallelize((1 to 8).map(x=>"n"+x), 10)  // 8个String，存储在10个分区中
> rdd1.zipPartitions(rdd2, true)((iter1, iter2) => {     // preservesPartitioning = true
> 		iter1
> 		  .zipAll(iter2, -1, "empty")		// 两个Partition进行zip操作，-1和"empty"是用于处理缺失值的dummy element
> 		  .map({case(x1, x2) => x1+"-"+x2}) // 将zip结果拼装成String
> }).collect()
> // res1: Array[String] = Array(1-empty, 2-n1, 3-n2, 4-n3, 5-n4, 6-empty, 7-n5, 8-n6, 9-n7, 10-n8)
> ~~~
>
> 例子中的Iterator是用来在Partition之间进行遍历的迭代器，如果需要改变Partition内的元素，可续使用`Iterator`的`drop`、`flatMap`等方法

### 4.3.2 排序（soriting）

> `sortByKey`、 `sortBy`、 `repartitionAndSortWithinPartition`等tranformation

#### (1) `sortBy`

> 例子如下
>
> ~~~scala
> // 按照商品名称对sortedProds排序
> // sortedProds : prod_id -> (prod_count, prod_info)
> // prod_info[1]: prod_name
> val sortedProds = totalsAndProds.sortBy(_._2._2(1)) 
> sortedProds.collect()
> // res0: Array[(Double, Array[String])] = Array((90,(48601.89,Array(90, AMBROSIA TRIFIDA POLLEN, 5887.49, 1))), (94,(31049.07,Array(94, ATOPALM MUSCLE AND JOINT, 1544.25, 7))), (87 (26047.72,Array(87, Acyclovir, 6252.58, 4))), ...
> ~~~
>
> 需要说明`sortBy`仅限于scala API，Java API不提供`sortBy`方法。在Java代码中，通常让Object实现Comparator接口，调用`sortByKey`方法即可

#### (2) `sortByKey`

##### (a) 方法签名

> 方法签名
>
> ~~~scala
> // Creates tuples of the elements in this RDD by applying f.
> def keyBy[K](f: (T) => K): RDD[(K, T)]
> 
> // Extra functions available on RDDs of (key, value) pairs where the key is sortable through an implicit conversion.
> // They will work with any key type K that has an implicit Ordering[K] in scope. 
> // Ordering objects already exist for all of the standard primitive types. 
> // Users can also define their own orderings for custom types, or to override the default ordering. 
> // The implicit ordering that is in the closest scope will be used.
> new OrderedRDDFunctions(self: RDD[P])(implicit arg0: Ordering[K], arg1: ClassTag[K], arg2: ClassTag[V], arg3: ClassTag[P])
> 
> // Sort the RDD by key, so that each partition contains a sorted range of the elements.
> def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length): RDD[(K, V)]
> ~~~

##### (b) 使用

> ~~~scala
> // 按照商品名称对sortedProds排序
> // sortedProds : prod_id -> (prod_count, prod_info)
> // prod_info[1]: prod_name
> val sortedProds2 = totalsAndProds
> 	.keyBy(_._2._2(1))
> 	.sortByKey()
> sortedProds2.collect()
> // res0: Array[(Double, Array[String])] = Array((90,(48601.89,Array(90, AMBROSIA TRIFIDA POLLEN, 5887.49, 1))), (94,(31049.07,Array(94, ATOPALM MUSCLE AND JOINT, 1544.25, 7))), (87 (26047.72,Array(87, Acyclovir, 6252.58, 4))), ...
> ~~~
>
> 说明：`sortByKey`和`repartitionAndSortWithinPartition`只能用于配有可排序Key的Pari RDD

##### (c) 让key变成ordable key

###### 方法1：使用`Ordered Trait`

> 例子如下
>
> ~~~scala
> case class Employee(lastName: String) extends Ordered[Employee] {
> 	// 0 for "=="; "<0" for "this < that"; ">0" for "this > that"
> 	override def compare(that: Employee) 
> 		= this.lastName.compare(that.lastName)
> }
> ~~~
>
> Employee扩展的是`Ordered Trait` ，而sortByKey要求key是`Ordering`类型
>
> 但是[Ordering](https://scala-lang.org/api/2.13.5/scala/math/Ordering$.html)提供了[隐式转换方法](https://docs.scala-lang.org/zh-cn/tour/implicit-conversions.html)，可以将Ordered类型转换成Ordering，方法签名如下
>
> ~~~scala
> // 使用了隐式参数的隐式转换方法
> // This would conflict with all the nice implicit Orderings available, 
> // but thanks to the magic of prioritized implicits via subclassing 
> // we can make Ordered[A] => Ordering[A] only turn up if nothing else works.
> implicit defordered[A](implicit asComparable: AsComparable[A]): Ordering[A]
> ~~~
>
> 反之亦然、[Ordered](https://kapeli.com/dash_share?docset_file=Scala&docset_name=Scala&path=scala/scala-library-2.13.5-javadoc/scala/math/Ordered$.html&platform=scala&repo=Main&version=2.13.5)也提供了隐式方法，将Ordering类型转换成Ordered

###### 方法2：使用`Ordering Trait`

> ~~~scala
> // 通用实现方式
> implicit val emplOrdering = new Ordering[Employee] {
> 	// 0 for "=="; "<0" for "left < right"; ">0" for "right > left"
> 	override def compare(a: Employee, b: Employee) =
> 		a.lastName.compare(b.lastName)
> }
> 
> // 如果用于比较的字段是`simple type`，可以用以下的方式比较
> implicit val emplOrdering: Ordering[Employee] = Ordering.by(_.lastName)
> ~~~
>
> 相关scala语法参考：[`implicit parameters`]()

#### (3) 使用`groupByKeyAndSortValues` 按照key分组并对value进行二级排序

方法签名及说明

> ~~~scala
> def groupByKeyAndSortValues[K: Ordering : ClassTag, V: ClassTag, S](
> 	rdd: RDD[(K,V)],
> 	secondaryKeyFunc: (V) => S,
> 	splitFunc: (V, V) => Boolean,
> 	numPartitions: Int
> ): RDD[(K, List[V])]
> ~~~
>
> 1. 只用于 `pair RDD` `
> 2. 需要一个`Ordering[V] object`在上下文中
> 3. 需要分区参数：Partitioner对象或者partition_num
> 4. 返回的`RDD[(K, List[V])]`为每个key配备一个`Iterable(V)`，可以按照使用隐式的`Ordering object`排序后的顺序迭代

关于只进行二级排序、不分组的方法，参考：[https://issues.apache.org/jira/browse/SPARK-3655](https://issues.apache.org/jira/browse/SPARK-3655)

#### (4) 使用 `top` 和 `takeOrdered` 来读取排序后的元素

`takeOrdered(n)` 和 `top(n)` 可以读取RDD中前n个（或者最后n个）元素

> * 需要在上下文中存在`Ordering[T]` 对象
> * 对于`PairRDD`来说，它不是对`key`来排序，而是对(K, V)`二元组来排序
> * 这两个方法被优化，它不需要进行全排序，因此比使用`sortBy`外加`take`更加高效

### 4.3.3 数据分组（Grouping）

> `aggregateByKey` (section 4.1.2), `groupByKey`, `groupBy`, `combineByKey`

#### (1) `groupByKey` 和 `groupBy`

方法签名

> ~~~scala
> def groupByKey()
> 	: RDD[(K, Iterable[V])]
> 
> def groupBy[K]
> 	(f: (T) => K)
> 	(implicit kt: ClassTag[K])
> : RDD[(K, Iterable[T])]
> ~~~

例子

> ~~~scala
> rdd.map(x => (f(x), x) /*转换成Pair RDD*/).groupByKey()
> rdd.groupBy(f)
> ~~~

说明

> * 这两个transformation都非常消耗内存，需要访问每个key的所有valu
> * 对于相对简单的场景，推荐使用 `aggregateByKey`, `reduceByKey`, `foldByKey` 这类不用进行full grouping的方法

#### (2) `combineByKey` 

> `combineByKey` is a generic transformation allows specify 1 customer function for merging `values` into `combined values` and anther 1 customer function for merging `combined values` themselves

方法签名

> ~~~scala
> def combineByKey[C](
> 	// 使用第一个Key的Value（类型V）创建Combined Value（类型C）
> 	createCombiner: V => C,	
> 	// 将一个Key的Value（类型V）合并到Combined Value（类型C）中
> 	mergeValue: (C, V) => C,	
> 	// 合并不同分区的Combined Value（类型C）
> 	mergeCombiners: (C, C)	=> C,	
> 	// 指定Patitioner、与父RDD相同时不会发生shuffling
> 	partitioner: Partitioner,		
> 	mapSideCombine: Boolean = true,
> 	// 默认使用spark.serializer中配置的Serializer
> 	serializer: Serializer = null
> ): RDD[(K, C)] 
> ~~~
>
> source code: [http://mng.bz/Z6fp](http://mng.bz/Z6fp) 

例子：演示 `aggregateByKey`, `groupByKey`, `foldByKey`, `reduceByKey` 这些方法是如何使用 `combinedByKey`来实现的

> ~~~scala
> // create the combined value (output) from the 1st key's value (t)
> // 		t(transaction): date, time, customer_id, prod_id, quantity, price
> // 		t(t):   price
> // 		t(4):   quantity
> // input:  transaction
> // output: (min_avg_price, max_avg_price, quantity, total)
> def createComb = (
> 	t:Array[String]) => {
> 		val total = t(5).toDouble 
> 		val q = t(4).toInt
> 		(total/q, total/q, q, total)}
> 
> // merge another key's value (V) in the same partition into the combined value (output)
> def mergeVal:(
> 	(Double,Double,Int,Double), Array[String]) => (Double,Double,Int,Double) = {
> 		case((mn,mx,c,tot), t) => {
> 			val total = t(5).toDouble
> 			val q = t(4).toInt
> 			(scala.math.min(mn,total/q), scala.math.max(mx,total/q), c+q, tot+total)
> 		}
> 	}
> 
> // merge combined values among partitions
> def mergeComb:(
> 	(Double,Double,Int,Double), (Double,Double,Int,Double)) => (Double,Double,Int,Double) = {
> 		case((mn1,mx1,c1,tot1),(mn2,mx2,c2,tot2)) 
> 			=> (scala.math.min(mn1,mn1), scala.math.max(mx1,mx2), c1+c2, tot1+tot2)
> 	} 
> 
> // transByCust: customer_id -> Array(transaction<date,time,customer_id,prod_id, quantity, price>)
> // invoking co
> val avgByCust = transByCust.combineByKey(
> 		createComb, // for creating the combined value from the 1st key's value
> 		mergeVal,   // for merging another key's valuein the same partition into the combined value
> 		mergeComb,  // for merging combined values among partitions
> 		// the partitioning is preserved by using the previous number of partitions #4
> 	  	new org.apache.spark.HashPartitioner(transByCust.partitions.size)
> 	).mapValues(
> 		# function parsed to RDD.mapValues
> 	 	{case(mn,mx,cnt,tot) => (mn,mx,cnt,tot,tot/cnt)}
> 	) 
> 	
> // test
> avgByCust.first()
> //		res0: (Int, (Double, Double, Int, Double, Double)) = (96,(856.2885714285715,4975.08,57,36928.57,647.869649122807))
> 
> // save to file
> totalsAndProds
> 	.map(_._2)
> 	.map(x=>x._2.mkString("#")+", "+x._1)
> 	.saveAsTextFile("ch04output-totalsPerProd")
> 
> avgByCust
> 	.map{case (id, (min, max, cnt, tot, avg)) 
> 		=> "%d#%.2f#%.2f#%d#%.2f#%.2f".format(id, min, max, cnt, tot, avg)}
> 	.saveAsTextFile("ch04output-avgByCust")
> ~~~

## 4.4 理解RDD依赖

>  `RDD依赖`、`RDD Check Point`是Spark Core API的核心实现机制

### 4.4.1. RDD依赖和Spark Job执行

#### 执行模式

> * 基于有向无环图（DAG）
>
> * 图的顶点是RDD、边时RDD之间的依赖
> * 执行一个transformation会创建一个新的顶点（即一个新的RDD）以及一条新的边（新的RDD依赖）

两种RDD间的依赖

(1) 窄依赖（Narrow Dependencies）：

> 两种窄依赖
>
> *  一对一依赖（one-to-one dependencies）
>
> * 范围依赖（range dependencies）：例如使用union transformation将几个父RDD组合起来

(2) 宽依赖（Wide Dependencies）：会发生shuffling

> 具体是否发生shuffling，参考4.4.2小节，另外Joining RDD的时候总是会发生shuffling

例子

> ~~~scala
> val list = List.fill(500)(scala.util.Random.nextInt(10))
> val listrdd = sc.parallelize(list, 5)
> val pairs = listrdd.map(x => (x, x*x))
> val reduced = pairs.reduceByKey((v1, v2)=>v1+v2)
> val finalrdd = reduced.mapPartitions(
>              iter => iter.map({case(k,v)=>"K="+k+",V="+v}))
> finalrdd.collect()
> ~~~

打印RDD DAG以用于性能调优或者debugging

> ~~~scala
> scala> println(finalrdd.toDebugString)
> (6) MapPartitionsRDD[4] at mapPartitions at <console>:20 []
>  |  ShuffledRDD[3] at reduceByKey at <console>:18 []
>  +-(5) MapPartitionsRDD[2] at map at <console>:16 []
>     |  ParallelCollectionRDD[1] at parallelize at <console>:14 []
> ~~~

### 4.2.2 Spark Job执行

> 代码被打包提交给Spark后的执行过程

每个Job都根据发生shuffle的时间点被分成多个阶段（Stages）

> * 对于每个`阶段`的每个`分区`，会创建一个Task并发给Executor
> * 阶段结束后对应的shuffle操作，被创建为一个shuffle-map Task

当一个Stage的所有Task都执行完毕后，进入下一个Stage，如此往复直到所有的Stage完成

之后进入启动最后一个Task，将计算结果打包发送给Driver（也叫做Result Task）

### 4.3.3 使用`Check Point`保存`RDD Lineage`

`RDD lineage`：RDD之间的依赖关系

`Check Point`：当向`SparkContext.setCheckpointDir()`设置HDFS Path或者Local Path之后，将会开启Spark的Check Point机制

> Spark可以将RDD、缓存、Lineage On Check Point保存在持久化的存储中，当有node failure发生时使用这些数据恢复计算状态


## 4.5 `Accumulators` 以及`broadcast variables` 

> `accumulators` 以及 `broadcast` 用来在Spark Executors之间通信，以维持global state或者所有task及partition之间的共享数据

### 4.5.1 使用Accumulator从Executor获取数据

> (1) shared across executors (2) can only add to (such as global sums and counters)

#### (1) Accumulator

> Accumulator被所有Executor共享，只支持add操作（例如计算global sum，或者全局计数器等），除了使用代码，它的值也可以展现在Spark Web UI上（第11章介绍）

例子

> ~~~scala
> val acc  = sc.accumulator(0, "acc name")
> val list = sc.parallelize(1 to 1000000)
> list.foreach(x => acc.add(1))
> 
> acc.value
> // res0: Int = 1000000
> 
> list.foreach(x => acc.value)
> // java.lang.UnsupportedOperationException: Can't read accumulator value in task
> ~~~

#### (2) 编写自定义Accumulator（使用`AccumulatorParam`和`Accumulable`）

`AccumulatorParam`

> 提供如下三个方法
>
> * `zero(initialValue: T)`: 传给Executor的初始值
> * `addInPlace(v1: T, v2: T):T`: 合并两个accumulated value
> * `addAccumulator(v1: T, v2: V): T`: 将一个value添加到Accumulator上
>
> Spark为`numeric type`提供`implicit AccumulatorParam`，但是对于其他类型，需要自己写一个AccumulatorParam object

`Accumulable`

> 自定义acuumulator，需要借助AccumulableParam来实现

例子

> ~~~scala
> val rdd = sc.parallelize(1 to 100)
> import org.apache.spark.AccumulableParam
> 
> // in (Int, Int): 1st Int is for tracking count, 2nd is for sum
> implicit object AvgAccParam extends AccumulableParam[(Int, Int), Int] {
> 	def zero(v:(Int, Int)) = (0, 0)
> 	def addInPlace(v1:(Int, Int), v2:(Int, Int))
>   		= (v1._1+v2._1, v1._2+v2._2)
> 	def addAccumulator(v1:(Int, Int), v2:Int)
>   		= (v1._1+1, 	v1._2+v2)
> }
> val acc = sc.accumulable((0,0))
> rdd.foreach(x => acc += x)
> val mean = acc.value._2.toDouble / acc.value._1
> // mean: Double = 50.5
> ~~~

#### (3) 使用Accumulable Collection来存储全局累加值

例子

> ~~~scala
> import scala.collection.mutable.MutableList
> 
> // 创建一个全局共享的mutable collection用来存储accumulated values
> val colacc = sc.accumulableCollection(MutableList[Int]())
> 
> rdd.foreach(x => colacc += x)
> colacc.value
> //	res0: 
> //		scala.collection.mutable.MutableList[Int]
> //	 	= MutableList(1, 2, 3, 4, 5, 6, 7, 8, 9, 
> //						 10, 31, 32, 33, ...)
> ~~~
>
> 实现简单，也不需要提供implicit object

### 4.5.2 Broadcast Variables

`Broadcast Variables`

> 用来将数据广播给所有的executors（例如业务配置等），由driver来提供，executor来读取

发送和读取Broadcast Variables

> ~~~scala
> sc.broadcast(value) //sc : SparkContext
> 
> // value必须是可以序列化的object
> // exector通过Broadcase.value方法读取数据
> ~~~

删除Broadcast Variables

> `destroy`：在driver以及executor上都删除
>
> `unpersist` ：仅删除executor上的缓存、仍然可以重新从driver发送到executor上。
>
> 当离开作用域时，Broadcast Variables会被自动unpersisted，因此不必显式地调用`unpersist`，仅需要在不使用时不再引用它即可

#### 配置参数

> * `spark.broadcast.compress`: whether compress before transfer (leave this at true)
> * `spark.io.compression.codec`: codec for compress
> * `spark.broadcast.blockSize`: size of the chunks of data used for transferring broadcast data. (probably leave this at the battle-tested default of 4096).
> * `spark.python.worker.reuse`: greatly affect broadcast performance in Python because, if workers aren’t reused, broadcast variables will need to be transferred for each task (You should keep this at true, which is the default value) .

## 4.6 Summary

> * `Pair RDDs` contain two-element tuples: `keys` and `values`.
> * Pair RDDs in Scala are implicitly converted to instances of class PairRDDFunctions, which hosts special pair RDD operations.
> * countByKey returns a map containing the number of occurrences of each key.
> * mapValues changes the values contained in a pair RDD without changing the associated keys.
> * flatMapValues enables you to change the number of elements corresponding to a key by mapping each value to zero or more values.
> * reduceByKey and foldByKey let you merge all the values of a key into a single value of the same type.
> * aggregateByKey merges values, but it also transforms values to another type.
> * Data partitioning is Spark’s mechanism for dividing data between multiple nodes in a cluster.
> * The number of RDD partitions is important because, in addition to influencing data distribution throughout the cluster, it also directly determines the number of tasks that will be running RDD transformations.
> * Partitioning of RDDs is performed by Partitioner objects that assign a partition index to each RDD element. Spark provides two implementations: HashPartitioner and RangePartitioner.
> * Physical movement of data between partitions is called shuffling. It occurs when data from multiple partitions needs to be combined in order to build partitions for a new RDD.
> * During shuffling, in addition to being written to disk, the data is also sent over the network, so it’s important to try to minimize the number of shuffles during Spark jobs.
> * RDD operations for working on partitions are mapPartitions and mapPartitionsWithIndex.
> * The four classic joins in Spark function just like the RDBMS joins of the same names: join (inner join), leftOuterJoin, rightOuterJoin, and fullOuterJoin.
> * cogroup performs grouping of values from several RDDs by key and returns an RDD whose values are arrays of Iterable objects containing values from each RDD.
> * The main transformations for sorting RDD data are sortByKey, sortBy, and repartitionAndSortWithinPartition.
> * Several pair RDD transformations can be used for grouping data in Spark: aggregateByKey, groupByKey (and the related groupBy), and combineByKey.
> * RDD lineage is expressed as a directed acyclic graph (DAG) connecting an RDD with its parent RDDs, from which it was transformed.
> * Every Spark job is divided into stages based on the points where shuffles occur.
> * The RDD lineage can be saved with checkpointing.
> * Accumulators and broadcast variables enable you to maintain a global state or share data across tasks and partitions in your Spark programs.