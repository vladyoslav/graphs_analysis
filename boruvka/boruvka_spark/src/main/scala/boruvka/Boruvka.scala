package boruvka

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Boruvka {

  case class MstEdge(src: Long, dst: Long, weight: Double)

  private case class CandidateEdge(
    src: Long,
    dst: Long,
    weight: Double,
    srcComp: Long,
    dstComp: Long
  )

  case class BoruvkaResult(
    edges: List[MstEdge],
    weight: Double,
    iterations: Int
  )

  private class UnionFind {
    private val parent = mutable.HashMap.empty[Long, Long]

    private def makeSet(x: Long): Unit = {
      if (!parent.contains(x)) parent(x) = x
    }

    def find(x: Long): Long = {
      makeSet(x)
      if (parent(x) != x) {
        parent(x) = find(parent(x))
      }
      parent(x)
    }

    def union(x: Long, y: Long): Boolean = {
      val rx = find(x)
      val ry = find(y)
      if (rx == ry) false
      else {
        parent(rx) = ry
        true
      }
    }
  }

  /**
   * Runs Boruvka's algorithm using GraphX/Spark primitives.
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
        val cheapestPerComponent = cheapestPerVertex
          .innerJoin(currentGraph.vertices) {
            (_, edgeData, compId) => (compId, edgeData)
          }
          .map { case (_, (compId, edgeData)) => (compId, edgeData) }
          .reduceByKey((a, b) => if (a._3 <= b._3) a else b)
          .cache()

        val candidateEdgesLocal: List[CandidateEdge] = cheapestPerComponent
          .values
          .keyBy { case (srcVertex, _, _) => srcVertex }
          .join(currentGraph.vertices)
          .map { case (_, ((srcVertex, dstVertex, weight), srcComp)) =>
            (dstVertex, (srcVertex, dstVertex, weight, srcComp))
          }
          .join(currentGraph.vertices)
          .map { case (_, ((srcVertex, dstVertex, weight, srcComp), dstComp)) =>
            val (s, d) =
              if (srcVertex < dstVertex) (srcVertex, dstVertex)
              else (dstVertex, srcVertex)

            val (c1, c2) =
              if (srcComp < dstComp) (srcComp, dstComp)
              else (dstComp, srcComp)

            CandidateEdge(s, d, weight, c1, c2)
          }
          .filter(e => e.srcComp != e.dstComp)
          .distinct()
          .collect()
          .toList

        if (debug) {
          println(s"[Boruvka] iteration $iterations: candidate edges = ${candidateEdgesLocal.size}")
        }

        if (candidateEdgesLocal.isEmpty) {
          cheapestPerVertex.unpersist()
          cheapestPerComponent.unpersist()

          if (debug) {
            val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
            println(s"[Boruvka] iteration $iterations finished: no candidate edges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
          }

          continueLoop = false
        } else {
          val uf = new UnionFind
          val acceptedCandidates = candidateEdgesLocal
            .sortBy(e => (e.weight, e.src, e.dst))
            .filter(e => uf.union(e.srcComp, e.dstComp))

          val acceptedEdgesLocal = acceptedCandidates.map(e => MstEdge(e.src, e.dst, e.weight))

          if (debug) {
            println(s"[Boruvka] iteration $iterations: accepted edges = ${acceptedEdgesLocal.size}")
          }

          if (acceptedEdgesLocal.isEmpty) {
            cheapestPerVertex.unpersist()
            cheapestPerComponent.unpersist()

            if (debug) {
              val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
              println(s"[Boruvka] iteration $iterations finished: no accepted edges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
            }

            continueLoop = false
          } else {
            mstResult = mstResult ++ acceptedEdgesLocal

            val acceptedMergeEdges: RDD[Edge[Int]] = sc.parallelize(
              acceptedCandidates.flatMap { e =>
                Seq(
                  Edge(e.srcComp, e.dstComp, 1),
                  Edge(e.dstComp, e.srcComp, 1)
                )
              }
            ).distinct().cache()

            val hasMerges = acceptedMergeEdges.take(1).nonEmpty

            if (!hasMerges) {
              cheapestPerVertex.unpersist()
              cheapestPerComponent.unpersist()
              acceptedMergeEdges.unpersist()

              if (debug) {
                val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
                println(s"[Boruvka] iteration $iterations finished: no accepted component merges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
              }

              continueLoop = false
            } else {
              val mergeVertices: RDD[(VertexId, Long)] = acceptedMergeEdges
                .flatMap(e => Seq(e.srcId, e.dstId))
                .distinct()
                .map(cid => (cid, cid))

              val componentMergeGraph = Graph(mergeVertices, acceptedMergeEdges, 0L)

              val mergedComponents: VertexRDD[VertexId] = componentMergeGraph
                .connectedComponents()
                .vertices
                .cache()

              val prevGraph = currentGraph

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
                  s"mst_edges_added=${acceptedEdgesLocal.size}, " +
                  s"mst_edges_total=${mstResult.size}, " +
                  s"remaining_edges=$currentEdges, " +
                  s"time_ms=${"%.2f".format(iterTimeMs)}"
                )
              }

              cheapestPerVertex.unpersist()
              cheapestPerComponent.unpersist()
              acceptedMergeEdges.unpersist()
              mergedComponents.unpersist()
              prevGraph.unpersist()
            }
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
