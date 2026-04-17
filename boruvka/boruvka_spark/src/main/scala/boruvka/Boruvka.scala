package boruvka

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

object Boruvka {

  case class MstEdge(src: Long, dst: Long, weight: Short)

  case class BoruvkaResult(edges: List[MstEdge], weight: Double, iterations: Int)

  private type EdgeMsg = ((Long, Long, Short), Long) // ((src, dst, weight), otherComp)

  /**
   * A fully deterministic comparison function for two potential MST edges.
   * It's used in both aggregateMessages and reduceByKey to ensure that for any
   * pair of choices, one is always deterministically picked.
   */
  private def minEdgeMerge(msg1: EdgeMsg, msg2: EdgeMsg): EdgeMsg = {
    val (edge1, otherComp1) = msg1
    val (edge2, otherComp2) = msg2
    val (src1, dst1, weight1) = edge1
    val (src2, dst2, weight2) = edge2

    if (weight1 < weight2) {
      msg1
    } else if (weight2 < weight1) {
      msg2
    } else if (otherComp1 < otherComp2) { // Tie-breaking by destination component ID
      msg1
    } else if (otherComp2 < otherComp1) {
      msg2
    } else if (dst1 < dst2) { // Tie-breaking by destination vertex ID
      msg1
    } else if (dst2 < dst1) {
      msg2
    } else if (src1 < src2) { // Final tie-breaking by source vertex ID
      msg1
    } else {
      msg2
    }
  }

  private class UnionFind {
    val parent = mutable.HashMap.empty[Long, Long]
    def find(x: Long): Long = {
      if (!parent.contains(x)) parent(x) = x
      if (parent(x) != x) parent(x) = find(parent(x))
      parent(x)
    }

    def union(x: Long, y: Long): Boolean = {
      val rootX = find(x); val rootY = find(y)
      if (rootX == rootY) false
      else { if (rootX < rootY) parent(rootY) = rootX else parent(rootX) = rootY; true }
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

  private def usedHeapMb(): Double = {
    val rt = Runtime.getRuntime
    (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0)
  }

  private def printMem(label: String, enabled: Boolean): Unit = {
    if (enabled) {
      println(f"[MEM] $label: used=${usedHeapMb()}%.2f MB")
    }
  }

  private def printEstimatedSize(label: String, obj: AnyRef, enabled: Boolean): Unit = {
    if (enabled) {
      val bytes = SizeEstimator.estimate(obj)
      val mb = bytes / 1024.0 / 1024.0
      println(f"[MEM] $label: estimated_size=${mb}%.2f MB")
    }
  }

  /**
   * Runs Boruvka's MST algorithm using GraphX.
   */
  def run(
    graph: Graph[Byte, Short],
    sc: SparkContext,
    debug: Boolean = false,
    profile: Boolean = false,
    mem: Boolean = false,
    sparkOnly: Boolean = false
  ): BoruvkaResult = {

    var currentGraph: Graph[Long, Short] = graph.mapVertices((vid, _) => vid).cache()
    currentGraph.vertices.count();
    printMem("after initial graph materialization", mem)

    val mstBuffer = mutable.ArrayBuffer.empty[MstEdge]
    var continueLoop = true
    var iterations = 0

    while (continueLoop) {
      iterations += 1
      val iterStart = System.nanoTime()
      if (debug) println(s"[Boruvka] iteration $iterations started")

      // Step 1: For each vertex, find the cheapest edge to another component.
      val cheapestPerVertex = timedStep(profile, s"iter $iterations - aggregateMessages") {
        currentGraph.aggregateMessages[EdgeMsg](
          ctx => {
            if (ctx.srcAttr != ctx.dstAttr) {
              val edge = (ctx.srcId, ctx.dstId, ctx.attr)
              ctx.sendToSrc((edge, ctx.dstAttr))
              ctx.sendToDst((edge, ctx.srcAttr))
            }
          },
          minEdgeMerge
        ).cache()
      }
      printMem(s"iter $iterations - after cheapestPerVertex materialization", mem)
      
      if (cheapestPerVertex.isEmpty()) {
        cheapestPerVertex.unpersist();
        if (debug) println(s"[Boruvka] iteration $iterations finished: no crossing edges, stopping.")
        continueLoop = false
      } else {
        // Step 2: For each component, select the single best edge leaving it.
        val cheapestPerComponent = timedStep(profile, s"iter $iterations - cheapestPerComponent") {
          cheapestPerVertex
            .innerJoin(currentGraph.vertices) { case (vid, msg, compId) => (compId, msg) }
            .map { case (_, (compId, msg)) => (compId, msg) }
            .reduceByKey(minEdgeMerge)
        }

        // Step 3: From all component choices, select one unique edge for each PAIR of components.
        val finalEdgesRdd = timedStep(profile, s"iter $iterations - finalEdgeSelection") {
            cheapestPerComponent.map { case (compA, (edge, compB)) =>
                val key = if (compA < compB) (compA, compB) else (compB, compA)
                (key, (edge, compA, compB))
            }.reduceByKey { case (candidate1, candidate2) =>
                if (minEdgeMerge((candidate1._1, candidate1._3), (candidate2._1, candidate2._3)) == ((candidate1._1, candidate1._3))) {
                    candidate1
                } else {
                    candidate2
                }
            }.values
            .cache()
        }

        if (finalEdgesRdd.isEmpty()) {
            cheapestPerVertex.unpersist();
            finalEdgesRdd.unpersist();
            if (debug) println(s"[Boruvka] iteration $iterations finished: no new edges to add.")
            continueLoop = false
        } else {
          var edgesToAddThisIteration: Array[(Long, Long, Short)] = Array.empty
          var collectedFinalEdges: Array[((Long, Long, Short), Long, Long)] = Array.empty
          
          // Step 4: Calculate component updates. This is the core logic that differs between modes.
          val compUpdateRdd: RDD[(VertexId, VertexId)] = 
            if (!sparkOnly) {
              // --- HYBRID MODE: Use local Union-Find on the driver ---
              timedStep(profile, "[DRIVER] Total Component Update") {
                
                collectedFinalEdges = timedStep(profile, "[DRIVER] Collect Final Edges") {
                  finalEdgesRdd.collect()
                }
                printMem("after [DRIVER] Collect Final Edges", mem)

                val componentUpdateMap = timedStep(profile, "[DRIVER] Build UF & Component Map") {
                  val uf = new UnionFind()
                  val allInvolvedCompIds = mutable.Set[Long]()
                  collectedFinalEdges.foreach { case (_, compA, compB) =>
                    uf.union(compA, compB)
                    allInvolvedCompIds.add(compA); allInvolvedCompIds.add(compB)
                  }
                  allInvolvedCompIds.map(id => id -> uf.find(id)).toMap
                }
                printEstimatedSize(s"iter $iterations - [DRIVER] componentUpdateMap", componentUpdateMap.asInstanceOf[AnyRef], mem)

                timedStep(profile, "[DRIVER] Parallelize Update Map") {
                    sc.parallelize(componentUpdateMap.toSeq)
                }
              }
            } else {
              // --- SPARK-ONLY MODE: Use connectedComponents on a graph of components ---
              timedStep(profile, "[SPARK] Total Component Update") {
                
                val mergeGraph = timedStep(profile, "[SPARK] Build Component Graph") {
                    val mergeEdges = finalEdgesRdd.map { case (_, compA, compB) => Edge(compA, compB, ()) }
                    val mergeVertices = currentGraph.vertices.map(_._2).distinct().map(id => (id, ()))
                    Graph(mergeVertices, mergeEdges)
                }
                printMem("after [SPARK] Build Component Graph", mem)

                val updates = timedStep(profile, "[SPARK] Run Connected Components") {
                    mergeGraph.connectedComponents().vertices
                }
                printMem("after [SPARK] Run Connected Components", mem)

                updates
              }
            }
          
          // In Spark-only mode, we need to collect the edges for the MST buffer *after*
          // the main component update logic has been timed.
          if (sparkOnly) {
            collectedFinalEdges = timedStep(profile, "Collect Edges for MST Buffer") {
              finalEdgesRdd.collect()
            }
          }

          edgesToAddThisIteration = collectedFinalEdges.map(_._1)

          mstBuffer ++= edgesToAddThisIteration.map { case (src, dst, w) => MstEdge(src, dst, w) }
          printEstimatedSize(s"iter $iterations - mstResult", mstBuffer.asInstanceOf[AnyRef], mem)

          // Step 5: Create a vertex-to-new-component mapping and apply it to the graph.
          val vertexToNewComponent = timedStep(profile, s"iter $iterations - [GRAPH] Build Vertex Update RDD") {
            currentGraph.vertices
              .map { case (vid, oldCompId) => (oldCompId, vid) }
              .join(compUpdateRdd)
              .map { case (_, (vid, newCompId)) => (vid, newCompId) }
          }

          val prevGraph = currentGraph
          
          // Define the new graph by relabeling vertices and pruning internal edges.
          // These are transformations, so no major computation happens here.
          currentGraph = timedStep(profile, s"iter $iterations - [GRAPH] Define New Graph (Relabel & Prune)") {
            currentGraph
              .outerJoinVertices(vertexToNewComponent)((_, oldCompId, newCompId) => newCompId.getOrElse(oldCompId))
              .subgraph(epred = e => e.srcAttr != e.dstAttr)
              .cache()
          }

          currentGraph = currentGraph
            .partitionBy(PartitionStrategy.EdgePartition2D)
            .cache()

          // Trigger an action to materialize the new graph and cache it.
          // This is where the actual computation for the above steps happens.
          timedStep(profile, s"iter $iterations - [GRAPH] Materialize New Graph") {
            currentGraph.vertices.count()
          }
          printMem(s"iter $iterations - after updated graph materialization", mem)
          
          if (debug || profile) {
            val iterTimeMs = (System.nanoTime() - iterStart) / 1e6
            println(f"[Boruvka] iteration $iterations finished: mst_edges_added=${edgesToAddThisIteration.length}, mst_edges_total=${mstBuffer.size}, time_ms=${"%.2f".format(iterTimeMs)}")
          }

          cheapestPerVertex.unpersist();
          finalEdgesRdd.unpersist();
          prevGraph.unpersist()
        }
      }
    }

    if (currentGraph != null) currentGraph.unpersist()

    BoruvkaResult(
      edges = mstBuffer.toList,
      weight = mstBuffer.view.map(_.weight.toDouble).sum,
      iterations = if (iterations > 0) math.max(1, iterations - 1) else 0
    )
  }
}
