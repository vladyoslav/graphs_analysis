package boruvka

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Boruvka {

  case class MstEdge(src: Long, dst: Long, weight: Double)

  case class BoruvkaResult(
    edges: List[MstEdge],
    weight: Double,
    iterations: Int
  )

  /**
   * Runs Boruvka's algorithm using GraphX/Spark primitives.
   *
   * Vertex attribute = current component id.
   * Edge attribute   = edge weight.
   */
  def run(graph: Graph[Long, Double], sc: SparkContext, debug: Boolean = false): BoruvkaResult = {
    var currentGraph = graph.mapVertices((vid, _) => vid)
    currentGraph.cache()
    currentGraph.vertices.count()

    var mstResult = List.empty[MstEdge]
    var continueLoop = true
    var iterations = 0

    while (continueLoop) {
      iterations += 1
      val iterStart = System.nanoTime()

      if (debug) {
        println(s"[Boruvka] iteration $iterations started")
      }

      // Step 1: for every vertex, find the cheapest outgoing edge
      val cheapestPerVertex: VertexRDD[(Long, Long, Double)] =
        currentGraph.aggregateMessages[(Long, Long, Double)](
          sendMsg = ctx => {
            if (ctx.srcAttr != ctx.dstAttr) {
              val edge = (ctx.srcId, ctx.dstId, ctx.attr)
              ctx.sendToSrc(edge)
              ctx.sendToDst(edge)
            }
          },
          mergeMsg = (a, b) => if (a._3 <= b._3) a else b
        ).cache()

      val hasCheapest = cheapestPerVertex.take(1).nonEmpty

      if (!hasCheapest) {
        cheapestPerVertex.unpersist()

        if (debug) {
          val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
          println(s"[Boruvka] iteration $iterations finished: no crossing edges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
        }

        continueLoop = false
      } else {

        // Step 2: choose one cheapest outgoing edge per component
        val cheapestPerComponent = cheapestPerVertex
          .innerJoin(currentGraph.vertices) {
            (_, edgeData, compId) => (compId, edgeData)
          }
          .map { case (_, (compId, edgeData)) => (compId, edgeData) }
          .reduceByKey((a, b) => if (a._3 <= b._3) a else b)
          .cache()

        // Step 3: normalize and collect newly selected result edges
        val newEdgesLocal = cheapestPerComponent.values
          .map { case (s, d, w) =>
            if (s < d) MstEdge(s, d, w) else MstEdge(d, s, w)
          }
          .distinct()
          .collect()
          .toList

        if (debug) {
          println(s"[Boruvka] iteration $iterations: selected ${newEdgesLocal.size} new edges")
        }

        if (newEdgesLocal.isEmpty) {
          cheapestPerVertex.unpersist()
          cheapestPerComponent.unpersist()

          if (debug) {
            val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
            println(s"[Boruvka] iteration $iterations finished: no new edges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
          }

          continueLoop = false
        } else {
          mstResult = mstResult ++ newEdgesLocal

          // Step 4: rebuild connected components from the real accumulated forest
          val forestEdges: RDD[Edge[Int]] = sc.parallelize(
            mstResult.flatMap { e =>
              List(
                Edge(e.src, e.dst, 1),
                Edge(e.dst, e.src, 1)
              )
            }
          )

          val forestVertices: RDD[(VertexId, Long)] = currentGraph.vertices.mapValues(_ => 0L)

          val updatedComponents: VertexRDD[VertexId] = Graph(forestVertices, forestEdges, 0L)
            .connectedComponents()
            .vertices
            .cache()

          val prevGraph = currentGraph

          // Step 5: update vertex component labels and remove intra-component edges
          currentGraph = currentGraph
            .outerJoinVertices(updatedComponents) {
              case (vid, _, Some(newCompId)) => newCompId
              case (vid, _, None)            => vid
            }
            .subgraph(epred = triplet => triplet.srcAttr != triplet.dstAttr)
            .cache()

          currentGraph.vertices.count()

          if (debug) {
            val currentEdges = currentGraph.edges.count()
            val iterTimeMs = (System.nanoTime() - iterStart) / 1e6

            println(
              s"[Boruvka] iteration $iterations finished: " +
              s"mst_edges_added=${newEdgesLocal.size}, " +
              s"mst_edges_total=${mstResult.size}, " +
              s"remaining_edges=$currentEdges, " +
              s"time_ms=${"%.2f".format(iterTimeMs)}"
            )
          }

          cheapestPerVertex.unpersist()
          cheapestPerComponent.unpersist()
          updatedComponents.unpersist()
          prevGraph.unpersist()
        }
      }
    }

    BoruvkaResult(
      edges = mstResult,
      weight = mstResult.map(_.weight).sum,
      iterations = iterations
    )
  }
}
