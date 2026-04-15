package boruvka

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

class BoruvkaTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setAppName("BoruvkaTest")
      .setMaster("local[2]")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  /** Creates an undirected GraphX graph from a list of (src, dst, weight). */
  private def makeGraph(edges: List[(Long, Long, Double)]): Graph[Byte, Short] = {
    val directed = edges.flatMap { case (s, d, w) =>
      val wShort = w.toShort
      List(Edge(s, d, wShort), Edge(d, s, wShort))
    }
    Graph.fromEdges(sc.parallelize(directed), 0.toByte)
  }

  /** Extracts all unique vertex IDs from MST edges. */
  private def verticesOf(edges: List[Boruvka.MstEdge]): Set[Long] =
    edges.flatMap(e => List(e.src, e.dst)).toSet

  /** Counts unique vertices in the original edge list. */
  private def vertexCount(edges: List[(Long, Long, Double)]): Int =
    edges.flatMap { case (s, d, _) => List(s, d) }.distinct.size


  // --- Basic correctness ---

  test("single edge: 2 vertices") {
    // Simplest possible graph: one edge
    // MST = that edge
    val graph = makeGraph(List((1L, 2L, 7.0)))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 7.0)
    assert(result.edges.size === 1)
  }

  test("triangle: picks two lightest edges") {
    //     1.0
    // 1 ────── 2
    //  \      /
    //  3\   /2
    //    \ /
    //     3
    // MST: 1-2 (1.0), 2-3 (2.0) = 3.0
    val graph = makeGraph(List(
      (1L, 2L, 1.0),
      (1L, 3L, 3.0),
      (2L, 3L, 2.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 3.0)
    assert(result.edges.size === 2)
    assert(verticesOf(result.edges) === Set(1L, 2L, 3L))
  }

  test("line graph: MST equals the whole graph") {
    // 1 ──5── 2 ──3── 3 ──7── 4
    // No alternative edges, MST = all edges
    val graph = makeGraph(List(
      (1L, 2L, 5.0),
      (2L, 3L, 3.0),
      (3L, 4L, 7.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 15.0)
    assert(result.edges.size === 3)
  }

  test("5-vertex graph: MST weight = 12.0") {
    val graph = makeGraph(List(
      (1L, 2L, 1.0),
      (1L, 3L, 3.0),
      (2L, 3L, 2.0),
      (3L, 4L, 5.0),
      (3L, 5L, 6.0),
      (4L, 5L, 4.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 12.0)
    assert(result.edges.size === 4)
    assert(verticesOf(result.edges) === Set(1L, 2L, 3L, 4L, 5L))
  }

  test("complete graph K4") {
    // All 4 vertices connected to each other
    // Kruskal order: 1-2(1) ✓, 2-3(2) ✓, 1-3(3) ✗ cycle, 2-4(4) ✓
    // MST = 1 + 2 + 4 = 7.0
    val graph = makeGraph(List(
      (1L, 2L, 1.0),
      (1L, 3L, 3.0),
      (1L, 4L, 5.0),
      (2L, 3L, 2.0),
      (2L, 4L, 4.0),
      (3L, 4L, 6.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 7.0)
    assert(result.edges.size === 3)
    assert(verticesOf(result.edges) === Set(1L, 2L, 3L, 4L))
  }


  // --- MST properties ---

  test("MST has exactly V-1 edges") {
    // Property: a spanning tree of V vertices always has V-1 edges
    val edgeList = List(
      (1L, 2L, 3.0), (1L, 3L, 1.0), (2L, 3L, 2.0),
      (2L, 4L, 5.0), (3L, 4L, 4.0), (4L, 5L, 6.0),
      (3L, 5L, 7.0), (1L, 5L, 8.0)
    )
    val graph = makeGraph(edgeList)
    val result = Boruvka.run(graph, sc)
    val v = vertexCount(edgeList)

    assert(result.edges.size === v - 1)
  }

  test("cycle property: heaviest edge in a cycle is not in MST") {
    // Triangle: edges 1-2(1), 2-3(2), 1-3(10)
    // The heaviest edge in the cycle (1-3, weight 10) must NOT be in MST
    val graph = makeGraph(List(
      (1L, 2L, 1.0),
      (2L, 3L, 2.0),
      (1L, 3L, 10.0)
    ))
    val result = Boruvka.run(graph, sc)

    val hasHeavyEdge = result.edges.exists { e =>
      (e.src == 1 && e.dst == 3) || (e.src == 3 && e.dst == 1)
    }
    assert(!hasHeavyEdge, "Heaviest edge in cycle should not be in MST")
  }

  test("all vertices covered") {
    // Every vertex in the graph must appear in at least one MST edge
    val edgeList = List(
      (10L, 20L, 1.0), (20L, 30L, 2.0),
      (30L, 40L, 3.0), (10L, 40L, 4.0),
      (10L, 30L, 5.0), (20L, 40L, 6.0)
    )
    val graph = makeGraph(edgeList)
    val result = Boruvka.run(graph, sc)
    val allVertices = edgeList.flatMap { case (s, d, _) => List(s, d) }.toSet

    assert(verticesOf(result.edges) === allVertices)
  }


  // --- Edge cases ---

  test("equal weights: any spanning tree is valid") {
    // All edges weight 1.0 — any spanning tree is MST
    // Just check V-1 edges and correct total weight
    val graph = makeGraph(List(
      (1L, 2L, 1.0),
      (2L, 3L, 1.0),
      (3L, 4L, 1.0),
      (1L, 3L, 1.0),
      (2L, 4L, 1.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.edges.size === 3)
    assert(result.weight === 3.0)
  }

  test("graph that is already a tree") {
    // Input has no redundant edges — MST = entire graph
    val graph = makeGraph(List(
      (1L, 2L, 5.0),
      (2L, 3L, 10.0),
      (3L, 4L, 15.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 30.0)
    assert(result.edges.size === 3)
  }

  test("non-sequential vertex IDs") {
    // Vertex IDs don't have to be 1,2,3... — can be any Long
    val graph = makeGraph(List(
      (100L, 200L, 1.0),
      (200L, 300L, 2.0),
      (100L, 300L, 5.0)
    ))
    val result = Boruvka.run(graph, sc)

    assert(result.weight === 3.0)
    assert(result.edges.size === 2)
    assert(verticesOf(result.edges) === Set(100L, 200L, 300L))
  }


  // --- Determinism ---

  test("two runs produce same result") {
    val graph = makeGraph(List(
      (1L, 2L, 1.0), (1L, 3L, 3.0), (2L, 3L, 2.0),
      (3L, 4L, 5.0), (3L, 5L, 6.0), (4L, 5L, 4.0)
    ))

    val r1 = Boruvka.run(graph, sc)
    val r2 = Boruvka.run(graph, sc)

    assert(r1.weight === r2.weight)
    assert(r1.edges.size === r2.edges.size)
  }
}
