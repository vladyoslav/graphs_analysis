package boruvka

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.io.Source

object Main {


  def main(args: Array[String]): Unit = {

    val inputPath = if (args.length >= 1) args(0)
                    else "../datasets/test_graph.mtx"

    val conf = new SparkConf()
      .setAppName("BoruvkaMST")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    try {

      val graph = loadMtxGraph(sc, inputPath)

      graph.cache()

      val numVertices = graph.vertices.count()
      val numEdges = graph.edges.count()

      val startTime = System.nanoTime()
      val (mstEdges, mstWeight) = boruvka(graph)
      val endTime = System.nanoTime()

      val timeMs = (endTime - startTime) / 1e6

      val graphName = inputPath.split("/").last
      println(s"graph: $graphName")
      println(s"vertices: $numVertices")
      println(s"edges: $numEdges")
      println(s"mst_weight: $mstWeight")
      println(s"mst_edges: ${mstEdges.length}")
      println(s"time_ms: $timeMs")

      println("--- MST edges ---")
      mstEdges.sortBy(_._3).foreach { case (u, v, w) =>
        println(s"  $u -- $v  (weight $w)")
      }

    } finally {
      sc.stop()
    }
  }

  def loadMtxGraph(sc: SparkContext, path: String): Graph[Long, Double] = {

    val allLines = Source.fromFile(path).getLines().toList

    val header = allLines.head.toLowerCase
    val isPattern = header.contains("pattern")
    val isSymmetric = header.contains("symmetric")

    val dataLines = allLines
      .filter(line => !line.startsWith("%"))
      .drop(1)

    val edges: List[Edge[Double]] = dataLines.flatMap { line =>
      val parts = line.trim.split("\\s+")
      if (parts.length < 2) {
        Nil
      } else {
        val src = parts(0).toLong
        val dst = parts(1).toLong

        if (src == dst) {
          Nil 
        } else {
          val weight: Double = if (!isPattern && parts.length >= 3) {
            parts(2).toDouble
          } else {
            val u = math.min(src, dst)
            val v = math.max(src, dst)
            ((u * 31 + v * 17) % 1000 + 1).toDouble
          }

          if (isSymmetric) {
            List(Edge(src, dst, weight), Edge(dst, src, weight))
          } else {
            List(Edge(src, dst, weight))
          }
        }
      }
    }

    Graph.fromEdges(sc.parallelize(edges), 0L)
  }

  def boruvka(
    graph: Graph[Long, Double]
  ): (Array[(Long, Long, Double)], Double) = {

    val allVertices: RDD[(VertexId, Long)] =
      graph.vertices.mapValues(_ => 0L)

    var g: Graph[Long, Double] = graph.mapVertices((vid, _) => vid)

    var mstEdges = List.empty[(Long, Long, Double)]

    var shouldContinue = true
    var iteration = 0

    while (shouldContinue) {
      iteration += 1

      val minEdgePerVertex: VertexRDD[(Long, Long, Double)] =
        g.aggregateMessages[(Long, Long, Double)](
          ctx => {
            if (ctx.srcAttr != ctx.dstAttr) {
              val info = (ctx.srcId, ctx.dstId, ctx.attr)
              ctx.sendToSrc(info)
              ctx.sendToDst(info)
            }
          },
          (a, b) => {
            if (a._3 < b._3) a
            else if (a._3 > b._3) b
            else {
              val aKey = (math.min(a._1, a._2), math.max(a._1, a._2))
              val bKey = (math.min(b._1, b._2), math.max(b._1, b._2))
              if (aKey._1 < bKey._1 || (aKey._1 == bKey._1 && aKey._2 < bKey._2)) a
              else b
            }
          }
        )

      if (minEdgePerVertex.isEmpty()) {
        shouldContinue = false
      } else {

        val compMinEdges: RDD[(Long, (Long, Long, Double))] =
          g.vertices
            .join(minEdgePerVertex)
            .map { case (_, (compId, edgeInfo)) => (compId, edgeInfo) }
            .reduceByKey { (a, b) =>
              if (a._3 < b._3) a
              else if (a._3 > b._3) b
              else {
                val aKey = (math.min(a._1, a._2), math.max(a._1, a._2))
                val bKey = (math.min(b._1, b._2), math.max(b._1, b._2))
                if (aKey._1 < bKey._1 || (aKey._1 == bKey._1 && aKey._2 < bKey._2)) a
                else b
              }
            }

        val newEdges: Array[(Long, Long, Double)] = compMinEdges
          .values
          .map { case (s, d, w) => if (s < d) (s, d, w) else (d, s, w) }
          .distinct()
          .collect()

        mstEdges = mstEdges ++ newEdges.toList

        val sc = g.vertices.sparkContext

        val mstEdgeRDD: RDD[Edge[Long]] = sc.parallelize(
          mstEdges.flatMap { case (s, d, _) =>
            List(Edge(s, d, 0L), Edge(d, s, 0L))
          }
        )

        val mstGraph = Graph(allVertices, mstEdgeRDD, 0L)
        val newComponents = mstGraph.connectedComponents().vertices

        g = g.outerJoinVertices(newComponents) {
          (vid, _, newCompOpt) => newCompOpt.getOrElse(vid)
        }
      }
    }

    val totalWeight = mstEdges.map(_._3).sum
    (mstEdges.toArray, totalWeight)
  }
}
