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

          // Step 4: build only the component-merge graph for this iteration
          val componentMergeEdges: RDD[Edge[Int]] = cheapestPerComponent
            .values
            .keyBy { case (srcVertex, _, _) => srcVertex }
            .join(currentGraph.vertices)
            .map { case (_, ((_, dstVertex, _), srcComp)) => (dstVertex, srcComp) }
            .join(currentGraph.vertices)
            .map { case (_, (srcComp, dstComp)) => (srcComp, dstComp) }
            .filter { case (srcComp, dstComp) => srcComp != dstComp }
            .flatMap { case (srcComp, dstComp) =>
              Seq(
                Edge(srcComp, dstComp, 1),
                Edge(dstComp, srcComp, 1)
              )
            }
            .distinct()
            .cache()

          val hasMerges = componentMergeEdges.take(1).nonEmpty

          if (!hasMerges) {
            cheapestPerVertex.unpersist()
            cheapestPerComponent.unpersist()
            componentMergeEdges.unpersist()

            if (debug) {
              val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
              println(s"[Boruvka] iteration $iterations finished: no component merges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
            }

            continueLoop = false
          } else {
            val mergeVertices: RDD[(VertexId, Long)] = componentMergeEdges
              .flatMap(e => Seq(e.srcId, e.dstId))
              .distinct()
              .map(cid => (cid, cid))

            val componentMergeGraph = Graph(mergeVertices, componentMergeEdges, 0L)

            val mergedComponents: VertexRDD[VertexId] = componentMergeGraph
              .connectedComponents()
              .vertices
              .cache()

            val prevGraph = currentGraph

            // Step 5: update each vertex's component label
            val vertexToNewComponent: RDD[(VertexId, VertexId)] = currentGraph.vertices
              .map { case (vertexId, oldCompId) => (oldCompId, vertexId) }
              .join(mergedComponents)
              .map { case (_, (vertexId, newCompId)) => (vertexId, newCompId) }

            currentGraph = currentGraph
              .outerJoinVertices(vertexToNewComponent) {
                case (_, oldCompId, Some(newCompId)) => newCompId
                case (_, oldCompId, None)            => oldCompId
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
            componentMergeEdges.unpersist()
            mergedComponents.unpersist()
            prevGraph.unpersist()
          }
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
