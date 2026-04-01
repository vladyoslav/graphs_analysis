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

    def roots(keys: Iterable[Long]): Map[Long, Long] = {
      keys.map(k => k -> find(k)).toMap
    }
  }

  /** Measures a step inside Boruvka when profiling is enabled. */
  private def timedStep[T](enabled: Boolean, label: String)(block: => T): T = {
    if (!enabled) block
    else {
      val start = System.nanoTime()
      val result = block
      val end = System.nanoTime()
      val ms = (end - start) / 1e6
      println(f"[PROFILE] $label: $ms%.2f ms")
      result
    }
  }

  /**
   * Runs Boruvka's algorithm using GraphX/Spark primitives.
   */
  def run(
    graph: Graph[Long, Double],
    sc: SparkContext,
    debug: Boolean = false,
    profile: Boolean = false
  ): BoruvkaResult = {
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
        timedStep(profile, s"iter $iterations - aggregateMessages") {
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
        }

      val hasCheapest = timedStep(profile, s"iter $iterations - check cheapest non-empty") {
        cheapestPerVertex.take(1).nonEmpty
      }

      if (!hasCheapest) {
        cheapestPerVertex.unpersist()

        if (debug) {
          val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
          println(s"[Boruvka] iteration $iterations finished: no crossing edges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
        }

        continueLoop = false
      } else {
        val cheapestPerComponent = timedStep(profile, s"iter $iterations - cheapestPerComponent") {
          cheapestPerVertex
            .innerJoin(currentGraph.vertices) {
              (_, edgeData, compId) => (compId, edgeData)
            }
            .map { case (_, (compId, edgeData)) => (compId, edgeData) }
            .reduceByKey((a, b) => if (a._3 <= b._3) a else b)
            .cache()
        }

        val componentMap = timedStep(profile, s"iter $iterations - collect component map") {
          currentGraph.vertices.collectAsMap()
        }

        val selectedEdges = timedStep(profile, s"iter $iterations - collect selected edges") {
          cheapestPerComponent.values.collect().toList
        }

        val candidateEdgesLocal = timedStep(profile, s"iter $iterations - build candidate edges locally") {
          selectedEdges
            .flatMap { case (srcVertex, dstVertex, weight) =>
              val srcComp = componentMap.get(srcVertex)
              val dstComp = componentMap.get(dstVertex)

              (srcComp, dstComp) match {
                case (Some(scid), Some(dcid)) if scid != dcid =>
                  val (s, d) =
                    if (srcVertex < dstVertex) (srcVertex, dstVertex)
                    else (dstVertex, srcVertex)

                  val (c1, c2) =
                    if (scid < dcid) (scid, dcid)
                    else (dcid, scid)

                  Some(CandidateEdge(s, d, weight, c1, c2))
                case _ =>
                  None
              }
            }
            .distinct
        }

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
          val acceptedCandidates = timedStep(profile, s"iter $iterations - filter accepted edges locally") {
            val uf = new UnionFind
            candidateEdgesLocal
              .sortBy(e => (e.weight, e.src, e.dst))
              .filter(e => uf.union(e.srcComp, e.dstComp))
          }

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

            val hasMerges = acceptedCandidates.nonEmpty

            if (!hasMerges) {
              cheapestPerVertex.unpersist()
              cheapestPerComponent.unpersist()

              if (debug) {
                val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
                println(s"[Boruvka] iteration $iterations finished: no accepted component merges, stopping (${ "%.2f".format(iterTimeMs) } ms)")
              }

              continueLoop = false
            } else {
              val mergedComponentsRdd: RDD[(VertexId, VertexId)] =
                timedStep(profile, s"iter $iterations - build component remap locally") {
                  val uf = new UnionFind
                  acceptedCandidates.foreach(e => uf.union(e.srcComp, e.dstComp))

                  val allComps = acceptedCandidates.flatMap(e => Seq(e.srcComp, e.dstComp)).distinct
                  val remap = uf.roots(allComps)
                  sc.parallelize(remap.toSeq)
                }

              val prevGraph = currentGraph

              val vertexToNewComponent: RDD[(VertexId, VertexId)] =
                timedStep(profile, s"iter $iterations - build vertex relabel map") {
                  currentGraph.vertices
                    .map { case (vertexId, oldCompId) => (oldCompId, vertexId) }
                    .join(mergedComponentsRdd)
                    .map { case (_, (vertexId, newCompId)) => (vertexId, newCompId) }
                }

              currentGraph = timedStep(profile, s"iter $iterations - relabel and prune graph") {
                currentGraph
                  .outerJoinVertices(vertexToNewComponent) {
                    case (_, oldCompId, Some(newCompId)) => newCompId
                    case (_, oldCompId, None)            => oldCompId
                  }
                  .subgraph(epred = triplet => triplet.srcAttr != triplet.dstAttr)
                  .cache()
              }

              timedStep(profile, s"iter $iterations - materialize updated graph") {
                currentGraph.vertices.count()
              }

              if (debug || profile) {
                val currentEdges = timedStep(profile, s"iter $iterations - count remaining edges") {
                  currentGraph.edges.count()
                }

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
