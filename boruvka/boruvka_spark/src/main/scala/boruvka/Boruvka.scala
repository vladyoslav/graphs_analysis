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
   * Runs Boruvka's algorithm to find the Minimum Spanning Tree.
   */
  def run(graph: Graph[Long, Double], sc: SparkContext): BoruvkaResult = {
    var currentGraph = graph.mapVertices((vid, _) => vid)
    currentGraph.cache()

    var mstResult = List.empty[MstEdge]
    var continueLoop = true
    var iterations = 0

    while (continueLoop) {
      iterations += 1

      // Find cheapest crossing edge for each vertex
      val cheapestPerVertex = currentGraph.aggregateMessages[(Long, Long, Double)](
        ctx => {
          if (ctx.srcAttr != ctx.dstAttr) {
            val msg = (ctx.srcId, ctx.dstId, ctx.attr)
            ctx.sendToSrc(msg)
            ctx.sendToDst(msg)
          }
        },
        (a, b) => if (a._3 <= b._3) a else b
      )

      if (cheapestPerVertex.isEmpty()) {
        continueLoop = false
      } else {
        // Aggregate per component
        val cheapestPerComponent = cheapestPerVertex
          .innerJoin(currentGraph.vertices) {
            (_, edgeData, compId) => (compId, edgeData)
          }
          .map { case (_, (compId, edgeData)) => (compId, edgeData) }
          .reduceByKey((a, b) => if (a._3 <= b._3) a else b)

        // Normalize and deduplicate
        val newEdges = cheapestPerComponent.values
          .map { case (s, d, w) =>
            if (s < d) MstEdge(s, d, w) else MstEdge(d, s, w)
          }
          .distinct()
          .collect()
          .toList

        if (newEdges.isEmpty) {
          continueLoop = false
        } else {
          mstResult = mstResult ++ newEdges

          // Rebuild component labels
          val mstEdgeRdd: RDD[Edge[Long]] = sc.parallelize(
            mstResult.flatMap(e =>
              List(Edge(e.src, e.dst, 0L), Edge(e.dst, e.src, 0L))
            )
          )
          val allVertices = currentGraph.vertices.mapValues(_ => 0L)
          val updatedComponents = Graph(allVertices, mstEdgeRdd, 0L)
            .connectedComponents()
            .vertices

          // Apply new labels and prune intra-component edges
          val prev = currentGraph
          currentGraph = currentGraph
            .outerJoinVertices(updatedComponents) {
              (vid, _, opt) => opt.getOrElse(vid)
            }
            .subgraph(t => t.srcAttr != t.dstAttr)

          currentGraph.cache()
          currentGraph.vertices.count()
          prev.unpersist()
        }
      }
    }

    BoruvkaResult(mstResult, mstResult.map(_.weight).sum, iterations)
  }
}
