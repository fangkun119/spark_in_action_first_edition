<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!--**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*-->

- [CH09 Spark GraphX](#ch09-spark-graphx)
  - [9.1. 使用GraphX API](#91-%E4%BD%BF%E7%94%A8graphx-api)
    - [9.1.1. 创建图对象：`Graph`](#911-%E5%88%9B%E5%BB%BA%E5%9B%BE%E5%AF%B9%E8%B1%A1graph)
    - [9.1.2. 图对象的Transforming操作](#912-%E5%9B%BE%E5%AF%B9%E8%B1%A1%E7%9A%84transforming%E6%93%8D%E4%BD%9C)
      - [(1) 对Edges / Vertics 进行Mapping操作](#1-%E5%AF%B9edges--vertics-%E8%BF%9B%E8%A1%8Cmapping%E6%93%8D%E4%BD%9C)
        - [`mapEdges`：对所有edge的属性进行修改](#mapedges%E5%AF%B9%E6%89%80%E6%9C%89edge%E7%9A%84%E5%B1%9E%E6%80%A7%E8%BF%9B%E8%A1%8C%E4%BF%AE%E6%94%B9)
        - [`mapVertices`：对所有顶点的属性进行修改](#mapvertices%E5%AF%B9%E6%89%80%E6%9C%89%E9%A1%B6%E7%82%B9%E7%9A%84%E5%B1%9E%E6%80%A7%E8%BF%9B%E8%A1%8C%E4%BF%AE%E6%94%B9)
        - [`mapTriplets`：借助`<src, edge, dst>`三元组的数据对所有edge的属性进行修改](#maptriplets%E5%80%9F%E5%8A%A9src-edge-dst%E4%B8%89%E5%85%83%E7%BB%84%E7%9A%84%E6%95%B0%E6%8D%AE%E5%AF%B9%E6%89%80%E6%9C%89edge%E7%9A%84%E5%B1%9E%E6%80%A7%E8%BF%9B%E8%A1%8C%E4%BF%AE%E6%94%B9)
      - [(2) `arrgegatingMessages`方法](#2-arrgegatingmessages%E6%96%B9%E6%B3%95)
      - [(3) `原始Graph` join `由arrgegatingMessages聚合得到的VertexRDD`](#3-%E5%8E%9F%E5%A7%8Bgraph-join-%E7%94%B1arrgegatingmessages%E8%81%9A%E5%90%88%E5%BE%97%E5%88%B0%E7%9A%84vertexrdd)
      - [(4) 图子集：选取图的一部分](#4-%E5%9B%BE%E5%AD%90%E9%9B%86%E9%80%89%E5%8F%96%E5%9B%BE%E7%9A%84%E4%B8%80%E9%83%A8%E5%88%86)
        - [(a) `subgraph`](#a-subgraph)
        - [(b) `mask`](#b-mask)
        - [(c) `filter`](#c-filter)
      - [(5) GraphX的Pregel Implementation](#5-graphx%E7%9A%84pregel-implementation)
  - [9.2. 使用Spark GraphX内置的图算法](#92-%E4%BD%BF%E7%94%A8spark-graphx%E5%86%85%E7%BD%AE%E7%9A%84%E5%9B%BE%E7%AE%97%E6%B3%95)
    - [9.2.1. 本章使用的数据集](#921-%E6%9C%AC%E7%AB%A0%E4%BD%BF%E7%94%A8%E7%9A%84%E6%95%B0%E6%8D%AE%E9%9B%86)
      - [(1) 数据集介绍](#1-%E6%95%B0%E6%8D%AE%E9%9B%86%E4%BB%8B%E7%BB%8D)
      - [(2) 加载数据集创建图](#2-%E5%8A%A0%E8%BD%BD%E6%95%B0%E6%8D%AE%E9%9B%86%E5%88%9B%E5%BB%BA%E5%9B%BE)
    - [9.2.2. 最短路径算法](#922-%E6%9C%80%E7%9F%AD%E8%B7%AF%E5%BE%84%E7%AE%97%E6%B3%95)
    - [9.2.3. Page Rank算法](#923-page-rank%E7%AE%97%E6%B3%95)
    - [9.2.4. Connected Components算法](#924-connected-components%E7%AE%97%E6%B3%95)
    - [9.2.5. Strongly Connected Components算法](#925-strongly-connected-components%E7%AE%97%E6%B3%95)
  - [9.3. 实现A*搜索算法](#93-%E5%AE%9E%E7%8E%B0a%E6%90%9C%E7%B4%A2%E7%AE%97%E6%B3%95)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CH09 Spark GraphX

> 原书章节：[https://livebook.manning.com/book/spark-in-action/chapter-9/](https://livebook.manning.com/book/spark-in-action/chapter-9/)
>
> 代码：[../ch09/scala/ch09-listings.scala](../ch09/scala/ch09-listings.scala)
>
> 内容：介绍Spark GraphX以及图算法API及例子（例如最短路径、Page Rank等），更详细的介绍（例如LDA、SVD算法等）可以参考《[GraphX in Action](https://images.manning.com/165/220/resize/book/8/2ab5574-0640-4779-a5b8-3da48cc92149/Malak-SG-HI.png)》这本书

## 9.1. 使用GraphX API

> Spark中的图是有向图，并且点和边都可以携带附加属性（例如：`点`可以包含网页url、title、日期等属性；`边`可以包含链接的描述信息）

### 9.1.1. 创建图对象：`Graph`

例子如下

> <div align="left"><img src="https://raw.githubusercontent.com/kenfang119/pics/main/upload/spark_graphx_creation_example.png" width="550" /></div>
>
> 顶点（ID、姓名、年龄）；边（关系）；
>
> 边可以是双向的，但出于简单考虑只保留了一个方向

构造图对象

>  ~~~scala
> import org.apache.spark.graphx._
> case class Person(name:String, age:Int)
> 
> // 顶点数组
> val vertices = sc.parallelize(Array(
> 	(1L, Person("Homer", 39)), (2L, Person("Marge", 39)), 
> 	(3L, Person("Bart", 12)),  (4L, Person("Milhouse", 12)))
> )
> 
> // 边数组
> val edges = sc.parallelize(Array(
> 	Edge(4L, 3L, "friend"), Edge(3L, 1L, "father"), 
> 	Edge(3L, 2L, "mother"), Edge(1L, 2L, "marriedTo"))
> )
> 
> // 用订单和边构建Graph
> val graph = Graph(vertices, edges)
>  ~~~

简单的图操作

> ~~~bash
> scala> graph.vertices.count()
> res0: Long = 4
> scala> graph.edges.count()
> res1: Long = 4
> ~~~

API说明

>  [`Graph`](https://github.com/apache/spark/blob/v2.4.5/graphx/src/main/scala/org/apache/spark/graphx/Graph.scala)类封装对顶点、边的访问，以及用于图形转换的各种操作
>
> * `VertexRDD`：二元组、包含Long类型的顶点ID以及任意类型的顶点属性
> * `EdgeRDD`：包含Edge对象，内含两个顶点ID（`srcId`，`dstId`）以及任意类型的边属性（`attr`）
>
> [`GraphOps`](https://github.com/apache/spark/blob/v2.4.5/graphx/src/main/scala/org/apache/spark/graphx/GraphOps.scala)也提供了操作Graph的API，在决定使用哪个API时，要同时考虑[`Graph`](http://mng.bz/078M)类以及[`GraphOps`](http://mng.bz/J4jv)类

例子

> ~~~scala
> val graph = Graph(users, relationships, defaultUser)
> val graph_operation=new GraphOps(graph)
> graph_operation.collectNeighborIds(EdgeDirection.Out).take(5).foreach(println)
> ~~~

### 9.1.2. 图对象的Transforming操作

#### (1) 对Edges / Vertics 进行Mapping操作

> 例如：将Edge的属性由String类型转换为Relationship Class，向edge属性添加更多数据等；或者对vertex的属性执行类似的操作；可以使用`mapEdges`或`mapVertices`

##### `mapEdges`：对所有edge的属性进行修改

> ~~~scala
> case class Relationship(relation:String)
> 
> // Graph[Person, Edge]，graph对象来自上面的代码
> val newGraph = graph.mapEdges( 
> 	// partId：partition-id
> 	// iter：用来遍历edge的迭代器
> 	(partId, iter) => iter.map(
> 		edge => Relationship(edge.attr)))
> ~~~
>
> 相比graph，返回的newGraph使用Relationship类作为边的属性
>
> ~~~
> scala> newgraph.edges.collect()
> res0: Array[org.apache.spark.graphx.Edge[Relationship]] =
> Array(Edge(3,1,Relationship(father)), ...)
> ~~~

##### `mapVertices`：对所有顶点的属性进行修改

> ~~~scala
> case class PersonExt(
>     name:String, age:Int, children:Int=0, friends:Int=0, married:Boolean=false)
> // 相比newGraph中顶点的类型Person
> // 返回newGraphExt中顶点的类型变成了PersonExt
> val newGraphExt = newGraph.mapVertices(
> 	(vid, person) =>
> 		PersonExt(person.name, person.age))
> ~~~

##### `mapTriplets`：借助`<src, edge, dst>`三元组的数据对所有edge的属性进行修改

> ~~~scala
> // 给graph每个节点增加属性，属性值为该节点的出度（out degree）
> val inputGraph: Graph[Int, String] =
> 	graph.outerJoinVertices(graph.outDegrees)(
> 		(vid, _, degOpt) => degOpt.getOrElse(0))
> 
> // 用每个三元组中src的属性来设置边的属性
> // Construct a graph where each edge contains the weight
> // and each vertex is the initial PageRank
> val outputGraph: Graph[Double, Double] =
> 	inputGraph
> 		.mapTriplets(triplet => 1.0 / triplet.srcAttr)
> 		.mapVertices((id, _) => 1.0)
> ~~~

#### (2) `arrgegatingMessages`方法

用途

> 在图的每个顶点上运行一个函数，并可选地向其相邻顶点发送Message。
>
> 该方法收集并汇总每个顶点收到的所有消息，并将它们存储在一个新的`VertexRDD`中

方法签名

> ~~~scala
> def aggregateMessages[A: ClassTag](
> 	sendMsg: EdgeContext[VD, ED, A] => Unit,
> 	mergeMsg: (A, A) => A,
> 	tripletFields: TripletFields = TripletFields.All)
> 	: VertexRDD[A]
> ~~~

方法参数：

> `sendMsg函数`：以`EdgeContext[VD, ED, A]`为参数，其中`EdgeContext`
>
> * 包含source vertex ID、destioation vertex ID、edge properties
> * 提供sendToSrc，sendToDst用来向指定的vertex发送消息
>
> `mergeMsg函数`：用于消息聚合
>
> `tripletFields（None, EdgeOnly, Src, Dst, All）`：用于指定哪些字段被放入消息中

例子：计算每个PersonExt的“子女数量”，“朋友数量”，“是否结婚”

> ~~~scala
> val aggVertices = newGraphExt.aggregateMessages(
> 	(ctx:EdgeContext[PersonExt, Relationship, Tuple3[Int, Int, Boolean]]) => {
> 		if(ctx.attr.relation == "marriedTo") { 
> 			ctx.sendToSrc((0, 0, true)); 
> 			ctx.sendToDst((0, 0, true)); 
> 		} else if(ctx.attr.relation == "mother" || ctx.attr.relation == "father") { 
> 			ctx.sendToDst((1, 0, false)); 
> 		} else if(ctx.attr.relation.contains("friend")) { 
> 			ctx.sendToDst((0, 1, false)); 
> 			ctx.sendToSrc((0, 1, false));
> 		}
> 	}, 
> 	(msg1:Tuple3[Int, Int, Boolean], msg2:Tuple3[Int, Int, Boolean]) 
> 		=> (msg1._1+msg2._1, msg1._2+msg2._2, msg1._3 || msg2._3)
> )
> ~~~
>
> 输出计算结果
>
> ~~~bash
> scala> aggVertices.collect.foreach(println)
> (4,(0,1,false))
> (2,(1,0,true))
> (1,(1,0,true))
> (3,(0,1,false))
> ~~~

#### (3) `原始Graph` join `由arrgegatingMessages聚合得到的VertexRDD`

通过VertexId，将额外的vertex message来Join到图中

方法签名

> ~~~scala
> // Currying语法：这个方法运行时，数据类型的变化过程是
> // RDD[(VertexId, U)] => (VertexId, VD, Option[U]) => Graph[VD2, ED]
> def outerJoinVertices[U:ClassTag, VD2:ClassTag]
> 	(other: RDD[(VertexId, U)])
> 	(mapFunc: (VertexId, VD, Option[U]) => VD2)
> 		: Graph[VD2, ED]
> ~~~

参数

> `other: RDD[(VertexId, U)]`：希望Join进来的Message，U即为Message的类型
>
> `mapFunc: (VertexId, VD, Option[U]) => VD2`：用来生成新的Vertex的函数

例子

> 将`子女数量`，`朋友数量`，`是否结婚`与原始图中的PersonExt进行Join，得到新的图并返回
>
> ~~~scala
> val graphAggr = newGraphExt.outerJoinVertices(
> 	// 阶段1：RDD[(VertexID, Aggregatedmessage)] => (VertexID, OriginalVertex, Option[AggregatedMessage])
> 	aggVertices // 类型是RDD[(VertexID, Aggregatedmessage)]
> )(
> 	// 阶段2：(VertexID, OriginalVertex, Option[AggregatedMessage]) => PersonExt
> 	(vid, origPerson, optMsg) => { 
> 		optMsg match {
> 			case Some(msg) => PersonExt(
> 				origPerson.name, origPerson.age, 
> 				msg._1, msg._2, msg._3 /*元组取值*/
> 			)
> 			case None => origPerson
> 	}}
> )
> ~~~

查看计算结果

> ~~~bash
> scala> graphAggr.vertices.collect().foreach(println)
> (4,PersonExt(Milhouse,12,0,1,false))
> (2,PersonExt(Marge,39,1,0,true))
> (1,PersonExt(Homer,39,1,0,true))
> (3,PersonExt(Bart,12,0,1,false))
> ~~~

#### (4) 图子集：选取图的一部分

三种方法

> * `subgraph` — 根据提供的谓词选择顶点和边
> * `mask` — 仅选择另一个图形中存在的顶点
> * `filter` — 前两者的结合

##### (a) `subgraph`

> 方法签名
>
> ~~~scala
> def subgraph(
> 	// 传入的两个谓词（predicate）：Edge PREDicate 和 Vertex PREDicate
> 	// epred函数可以通过EdgeTriplet得到边以及两个顶点的信息，来判断边是否需要保留
> 	epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
> 	// vped可以得到顶点的信息，来判断顶点是否需要保留
> 	vpred: (VertexId, VD) => Boolean = ((v, d) => true)
> ) : Graph[VD, ED]
> ~~~
>
> 例子
>
> ~~~scala
> val parents = graphAggr.subgraph(
> 	_ => true, // 不对边设置裁剪规则，它只随着顶点的裁剪而被裁剪
> 	(vertexId, person) => person.children > 0 // 顶点只保留children > 0的
> )
> ~~~
>
> 查看运行结果
>
> ~~~bash
> scala> parents.vertices.collect.foreach(println)
> (1,PersonExt(Homer,39,1,0,true))
> (2,PersonExt(Marge,39,1,0,true))
> scala> parents.edges.collect.foreach(println)
> Edge(1,2,Relationship(marriedTo))
> ~~~

##### (b) `mask`

> 将一张图投射到另一张图上，只保留也在另一张图中也存在的顶点和边（只考虑顶点和边、不考虑他们携带的属性）。方法签名如下
>
> ~~~scala
> // restricts the graph to only the vertices and edges that are also in other, 
> // but keeps the attributes from this graph.
> def mask[VD2, ED2](
> 	other: Graph[VD2, ED2]
> ) (
> 	implicit arg0: ClassTag[VD2], arg1: ClassTag[ED2]
> ): Graph[VD, ED]
> ~~~

##### (c) `filter`

> 上述两个方法的结合
>
> ~~~scala
> // filter the graph by computing some values to filter on, 
> // and applying the predicates.
> def filter[VD2, ED2](
> 	// 现将preprocess传入的图当做mask来得到一个子图
> 	preprocess	: (Graph[VD, ED]) => Graph[VD2, ED2], 
> 	// 再用epred、vpred函数对子图进行裁剪
> 	epred		: (EdgeTriplet[VD2, ED2]) => Boolean = (x: EdgeTriplet[VD2, ED2]) => true, 
> 	vpred		: (VertexId, VD2) => Boolean = (v: VertexId, d: VD2) => true
> )(
> 	implicit arg0: ClassTag[VD2], arg1: ClassTag[ED2]
> ): Graph[VD, ED]
> ~~~

#### (5) GraphX的Pregel Implementation

> Pregel是谷歌的图形处理框架（Grzegorz Malewicz et al., “Pregel: A System for Large-Scale Graph Processing,” https://kowshik.github.io/），而GraphX提供了类似Pregel的API

Superstep：Pregel 的执行由一组superstep组成，通过Pregel对象的apply方法执行

方法的签名如下

> ~~~scala
> // VD: 顶点；ED：边；A：消息
> def apply[VD: ClassTag, ED: ClassTag, A: ClassTag] (
> 	graph: Graph[VD, ED], // 输入的Graph
> 	initialMsg: A,		  // 初始superstep中发往所有节点的消息
> 	maxIterations: Int = Int.MaxValue, // 最大迭代次数
> 	// 调用sendMsg时，向哪个方向发送消息，取值包括
> 	// * EdgeDirection.out
> 	// * EdgeDirection.In
> 	// * EdgeDirection.Either
> 	// * EdgeDirection.Both
> 	activeDirection: EdgeDirection = EdgeDirection.Either
> ) (
> 	// 顶点收到消息时执行vprog函数，可以修改顶点的内容
> 	vprog: (VertexId, VD, A) => VD,
> 	// 发送消息：输入是三元组（2顶点+1条边），输出是消息迭代器，定义向每个顶点发送的消息
> 	sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
> 	// 合并发往同一个Vertex的消息
> 	mergeMsg: (A, A) => A
> ) : Graph[VD, ED]
> ~~~

其中的initial superstep会在所有顶点上执行，而后续superstep只会在收到消息的顶点上执行，当没有新的消息被发出或者到达最大迭代次数时，方法执行结束

## 9.2. 使用Spark GraphX内置的图算法

> 本章介绍几种Spark图算法
>
> * `最短路径`
> * `Page Ranking`
> * `Connected Components`
> * `Strongly Connected Components`（doublly connected vertices）
>
> 更多的图算法，例如`三角形计数`、`SVD++`、`协同过滤`、`Latent Dirichlet`（LDA）
>
> 参考另一本书 "Spark GraphX in Action"

### 9.2.1. 本章使用的数据集

#### (1) 数据集介绍

> 数据集来自项目“Human Wayfinding in Information Networks”，基于一个叫做"[Wikispeedia](http://snap.stanford.edu/data/wikispeedia.html)"的在线游戏。该游戏包含了一组Wikipedia文章、要求玩家用尽可能少的link来对文章进行关联。包含两个文件：
>
> [`articles.tsv`](https://github.com/fangkun119/spark_in_action_first_edition/blob/master/ch09/articles.tsv)：1行1个文章名称
>
> [`links.tsv`](https://raw.githubusercontent.com/fangkun119/spark_in_action_first_edition/master/ch09/links.tsv)：1行1个link，格式为`"${src_article_title}\t${dest_article_title}"`

#### (2) 加载数据集创建图

> ~~~scala
> // 加载文章：一行一个标题
> val articles = sc
> 	.textFile("first-edition/ch09/articles.tsv", 6) 			// 加载文章
> 	.filter(line => line.trim() != "" && !line.startsWith("#")) // 过滤空行和注释
> 	.zipWithIndex() // title => (title, uniqueID) 	// def zipWithIndex(): RDD[(T, Long)]
> 	.cache() 		// 缓存
> 
> // 加载链接：SRC文章标题\tDST文章标题
> val links = sc
> 	.textFile("first-edition/ch09/links.tsv", 6) 				// 加载
> 	.filter(line => line.trim() != "" && !line.startsWith("#")) // 过滤空行和注释
> 
> val linkIndexes = links
> 	.map(x => { 
> 		// 创建Pair RDD: (src_title, dest_title)
> 		val spl = x.split("\t"); (spl(0), spl(1)) 
> 	}) 
> 	// 将src_title, dest_title都替换成文章的unique_id
> 	// def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
> 	.join(articles).map(x => x._2)	
> 	.join(articles).map(x => x._2)
> 
> // 创建Graph
> // def fromEdgeTuples[VD](rawEdges: RDD[(VertexId, VertexId)], defaultValue: VD, ...)
> val wikigraph = Graph.fromEdgeTuples(linkIndexes, 0)
> 
> articles.count()
> // Long = 4604，文章总数量
> wikigraph.vertices.count()
> // Long = 4592，图中vertex的数量
> linkIndexes.map(x => x._1).union(linkIndexes.map(x => x._2)).distinct().count()
> // Long = 4592，与链接向连的vertex的数量
> ~~~

### 9.2.2. 最短路径算法

最短路径算法

> 给一个起始顶点，最短路径算法图中每一个顶点到达这个起始顶点所需要遵循的最小边

代码

> 找到两个文章的顶点ID
>
> ~~~scala
> articles.
> 	// 标题为Rainbow或者14th_centry的文章
> 	filter(x => x._1 == "Rainbow" || x._1 == "14th_century").
> 	collect().
> 	foreach(println)
> // 找到文章ID：10和3425
> // (14th_century,10) 
> // (Rainbow,3425)
> ~~~
>
> 调用最短路径算法
>
> ~~~scala
> import org.apache.spark.graphx.lib._
> val shortest = ShortestPaths.run(wikigraph, Seq(10)) // 文章14th_century
> ~~~
>
> 查找计算结果
>
> ~~~scala
> scala> shortest.vertices.filter(x => x._1 == 3425).collect.foreach(println)
> (3425,Map(1772 -> 2)) // 需要两个链接可以跳转到
> ~~~

### 9.2.3. Page Rank算法

> ~~~scala
> val ranked = wikigraph.pageRank(0.001)
> 
> val ordering = new Ordering[Tuple2[VertexId,Double]] {
>   def compare(x:Tuple2[VertexId, Double], y:Tuple2[VertexId, Double]):Int =
>     x._2.compareTo(y._2) }
> val top10 = ranked.vertices.top(10)(ordering)
> 
> sc.parallelize(top10).join(articles.map(_.swap)).collect.
>   sortWith((x, y) => x._2._1 > y._2._1).foreach(println)
> // (4297,(43.064871681422574,United_States))
> // (1568,(29.02695420077583,France))
> // (1433,(28.605445025345137,Europe))
> // (4293,(28.12516457691193,United_Kingdom))
> // (1389,(21.962114281302206,English_language))
> // (1694,(21.77679013455212,Germany))
> // (4542,(21.328506154058328,World_War_II))
> // (1385,(20.138550469782487,England))
> // (2417,(19.88906178678032,Latin))
> // (2098,(18.246567557461464,India))
> ~~~
>
> 代码说明：[https://livebook.manning.com/book/spark-in-action/chapter-9/133](https://livebook.manning.com/book/spark-in-action/chapter-9/133)

### 9.2.4. Connected Components算法

> 查看图有几个互相不连通的子图组成（只要有一条边连接两个顶点，就认为这两个顶点是连通的）
>
> ~~~scala
> val wikiCC = wikigraph.connectedComponents()
> 
> wikiCC.vertices.
> 	map(x => (x._2, x._2)).
> 	distinct().
> 	join(articles.map(_.swap)).
> 	collect.
> 	foreach(println)
> // 两个子图：子图中值最小的Vertex ID以及Vertex内容
> // (0,(0,%C3%81ed%C3%A1n_mac_Gabr%C3%A1in))
> // (1210,(1210,Directdebit))
> 
> wikiCC.
> 	vertices.
> 	map(x => (x._2, x._2)).
> 	countByKey().
> 	foreach(println)
> // 每个子图中有多少个顶点
> // (0,4589)
> // (1210,3) // 第二个子图只有3个顶点，说明这个图的连通性良好
> ~~~

### 9.2.5. Strongly Connected Components算法

> 查看图有几个强连通的子图组成（两个顶点必须有双向连接，才认为它们是连通的）
>
> ~~~scala
> val wikiSCC = wikigraph.stronglyConnectedComponents(100)
> 
> wikiSCC.vertices.map(x => x._2).distinct.count
> // 共有519个强连通的子图
> // res0: Long = 519 
> 
> wikiSCC.vertices.map(x => (x._2, x._1)).countByKey().
>   filter(_._2 > 1).toList.sortWith((x, y) => x._2 > y._2).foreach(println)
> // (6,4051)
> // (2488,6)
> // (1831,3)
> // (892,2)
> (1950,2)
> (4224,2)
> ...
> 
> // 查看这些子图中的顶点
> wikiSCC.vertices.filter(x => x._2 == 2488).
> 	join(articles.map(x => (x._2, x._1))).collect.foreach(println)
> // (2490,(2488,List_of_Asian_countries))
> // (2496,(2488,List_of_Oceanian_countries))
> // (2498,(2488,List_of_South_American_countries))
> // (2493,(2488,List_of_European_countries))
> // (2488,(2488,List_of_African_countries))
> // (2495,(2488,List_of_North_American_countries))
> 
> wikiSCC.vertices.filter(x => x._2 == 1831).
> 	join(articles.map(x => (x._2, x._1))).collect.foreach(println)
> // (1831,(1831,HD_217107))
> // (1832,(1831,HD_217107_b))
> // (1833,(1831,HD_217107_c))
> 
> wikiSCC.vertices.filter(x => x._2 == 892).
>   join(articles.map(x => (x._2, x._1))).collect.foreach(println)
> // (1262,(892,Dunstable_Downs))
> // (892,(892,Chiltern_Hills))
> ~~~

## 9.3. 实现A*搜索算法

> todo

9.3.1. Understanding the A* algorithm

> todo

9.3.2. Implementing the A* algorithm

> 略

9.3.3. Testing the implementation

> 略


