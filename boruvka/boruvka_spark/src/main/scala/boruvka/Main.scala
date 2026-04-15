package boruvka

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD  // ← ДОБАВЛЕНО

import java.io.{File, PrintWriter}

object Main {

  case class GraphMeta(vertices: Long, edges: Long)

  /** Counts connected components in an undirected graph. */
  def countConnectedComponents(graph: Graph[Byte, Short]): Long = {
    graph.connectedComponents().vertices.map(_._2).distinct().count()
  }

  /** Builds an undirected GraphX graph from result edges. */
  def buildMstGraph(sc: SparkContext, edges: List[Boruvka.MstEdge], numVertices: Long): Graph[Byte, Short] = {  // ← ИЗМЕНЕНО
    // Создаём ВСЕ вершины явно
    val vertices: RDD[(VertexId, Byte)] = sc.parallelize(
      (1L to numVertices).map(id => (id, 0.toByte))
    )
    
    val graphEdges = sc.parallelize(
      edges.flatMap { e =>
        List(
          Edge(e.src, e.dst, 1.toShort),
          Edge(e.dst, e.src, 1.toShort)
        )
      }
    )
    
    Graph(vertices, graphEdges)  // ← ИЗМЕНЕНО
  }

  /** Counts unique vertices covered by result edges. */
  def countCoveredVertices(edges: List[Boruvka.MstEdge]): Int = {
    edges.flatMap(e => List(e.src, e.dst)).distinct.size
  }

  /** Measures and prints execution time of a named phase. */
  def timed[T](label: String)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    val ms = (end - start) / 1e6
    println(f"[TIME] $label: $ms%.2f ms")
    result
  }

  def usedHeapMb(): Double = {
    val rt = Runtime.getRuntime
    (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0)
  }

  def maxHeapMb(): Double = {
    Runtime.getRuntime.maxMemory() / (1024.0 * 1024.0)
  }

  def printHeap(label: String, enabled: Boolean): Unit = {
    if (enabled) {
      println(f"[MEM] $label: used=${usedHeapMb()}%.2f MB / max=${maxHeapMb()}%.2f MB")
    }
  }

  def forceGc(label: String, enabled: Boolean): Unit = {
    if (enabled) {
      println(s"[MEM] forcing GC: $label")
      System.gc()
      Thread.sleep(2000)
      printHeap(s"after forced gc - $label", enabled)
    }
  }

  /**
   * Parses command-line arguments.
   *
   * Supported forms:
   *   --key=value
   *   --debug
   *   --checks
   *   --profile
   *   --mem
   */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      if (arg == "--debug") {
        Some("debug" -> "true")
      } else if (arg == "--checks") {
        Some("checks" -> "true")
      } else if (arg == "--profile") {
        Some("profile" -> "true")
      } else if (arg == "--mem") {
        Some("mem" -> "true")
      } else if (arg == "--sparkOnly") {
        Some("sparkOnly" -> "true")
      } else if (arg.startsWith("--") && arg.contains("=")) {
        val parts = arg.drop(2).split("=", 2)
        Some(parts(0) -> parts(1))
      } else {
        None
      }
    }.toMap
  }

  /** Reads graph metadata from Matrix Market header/size line. */
  private def readMtxMeta(sc: SparkContext, path: String): GraphMeta = {
    val headLines = sc.textFile(path).take(200)

    def isIntegerToken(s: String): Boolean = s.forall(_.isDigit)

    val metaLineOpt = headLines.iterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        if (line.startsWith("%")) line.drop(1).trim else line
      }
      .filter(_.nonEmpty)
      .find { line =>
        val parts = line.split("\\s+")
        parts.length == 3 && parts.forall(isIntegerToken)
      }

    val metaLine = metaLineOpt.getOrElse {
      throw new IllegalArgumentException(s"Could not find Matrix Market size line in $path")
    }

    val parts = metaLine.split("\\s+")
    GraphMeta(vertices = parts(0).toLong, edges = parts(2).toLong)
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val inputPath = params.getOrElse("graph", {
      System.err.println("Error: --graph is required.")
      System.err.println("Usage: BoruvkaMST --graph=<path> [--csv=<path>] [--runs=<n>] [--warmup=<n>] [--cores=<n>] [--debug] [--checks] [--profile] [--mem]")
      System.err.println("  --graph    path to .mtx file (required)")
      System.err.println("  --csv      path to save CSV results (optional)")
      System.err.println("  --runs     number of benchmark runs (default: 5)")
      System.err.println("  --warmup   number of warmup runs (default: 3)")
      System.err.println("  --cores    number of CPU cores (default: * = all)")
      System.err.println("  --debug    enable per-iteration debug log (optional flag)")
      System.err.println("  --checks   enable final correctness checks (optional flag)")
      System.err.println("  --profile  enable per-step profiling inside Boruvka (optional flag)")
      System.err.println("  --mem      enable memory profiling (optional flag)")
      System.err.println("  --sparkOnly      enable Spark only implementation")
      System.exit(1)
      ""
    })

    val inputFile = new File(inputPath)
    if (!inputFile.exists()) {
      System.err.println(s"Error: file not found: $inputPath")
      System.exit(1)
    }
    if (!inputFile.isFile) {
      System.err.println(s"Error: not a file: $inputPath")
      System.exit(1)
    }

    val csvOutputPath = params.get("csv")
    csvOutputPath.foreach { path =>
      val parentDir = new File(path).getParentFile
      if (parentDir != null && !parentDir.exists()) {
        System.err.println(s"Error: directory does not exist: ${parentDir.getPath}")
        System.exit(1)
      }
    }

    val numRuns = try {
      params.getOrElse("runs", "5").toInt
    } catch {
      case _: NumberFormatException =>
        System.err.println(s"Error: --runs must be a positive integer, got: ${params("runs")}")
        System.exit(1)
        0
    }
    if (numRuns <= 0) {
      System.err.println(s"Error: --runs must be a positive integer, got: $numRuns")
      System.exit(1)
    }

    val numWarmup = try {
      params.getOrElse("warmup", "3").toInt
    } catch {
      case _: NumberFormatException =>
        System.err.println(s"Error: --warmup must be a non-negative integer, got: ${params("warmup")}")
        System.exit(1)
        0
    }
    if (numWarmup < 0) {
      System.err.println(s"Error: --warmup must be a non-negative integer, got: $numWarmup")
      System.exit(1)
    }

    val numCores = params.getOrElse("cores", "*")
    if (numCores != "*") {
      try {
        val n = numCores.toInt
        if (n <= 0) {
          System.err.println(s"Error: --cores must be a positive integer or *, got: $numCores")
          System.exit(1)
        }
      } catch {
        case _: NumberFormatException =>
          System.err.println(s"Error: --cores must be a positive integer or *, got: $numCores")
          System.exit(1)
      }
    }

    val debug = params.get("debug").contains("true")
    val checks = params.get("checks").contains("true")
    val profile = params.get("profile").contains("true")
    val mem = params.get("mem").contains("true")
    val sparkOnly = params.get("sparkOnly").contains("true")

    val conf = new SparkConf()
      .setAppName("BoruvkaMST")
      .setMaster(s"local[$numCores]")
      .set("spark.ui.enabled", "false")
      .set("spark.eventLog.enabled", "false")
      .set("spark.driver.memory", "16g")
      .set("spark.executor.memory", "16g")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val actualCores = sc.defaultParallelism
    var graph: Graph[Byte, Short] = null

    try {
      printHeap("before load graph", mem)

      val meta = timed("read graph metadata") {
        readMtxMeta(sc, inputPath)
      }

      graph = timed("load graph") {
        loadMtxGraph(sc, inputPath, meta)  // ← ИЗМЕНЕНО: передаём meta
      }

      printHeap("after load graph", mem)

      val inputComponents =
        if (checks) {
          val c = timed("count input components") {
            countConnectedComponents(graph)
          }
          printHeap("after count input components", mem)
          forceGc("gc after count input components", mem)
          c
        } else {
          -1L
        }

      val expectedResultEdges =
        if (checks) meta.vertices - inputComponents
        else -1L

      val graphName = inputFile.getName

      println()
      println("=== INPUT ===")
      println(s"graph:              $graphName")
      println(s"vertices:           ${meta.vertices}")
      println(s"edges:              ${meta.edges}")
      if (checks) {
        println(s"components:         $inputComponents")
        println(s"expected_mst_edges: $expectedResultEdges")
      } else {
        println(s"components:         skipped")
        println(s"expected_mst_edges: skipped")
      }
      println(s"cores:              $actualCores")
      println(s"warmup_runs:        $numWarmup")
      println(s"benchmark_runs:     $numRuns")
      println(s"debug:              $debug")
      println(s"checks:             $checks")
      println(s"profile:            $profile")
      println(s"mem:                $mem")
      println(s"sparkOnly:          $sparkOnly")

      forceGc("before warmup runs", mem)

      for (i <- 0 until numWarmup) {
        val warmup = timed(s"warmup ${i + 1}/$numWarmup") {
          Boruvka.run(graph, sc, debug, profile, mem, sparkOnly)
        }
        println(s"Warmup ${i + 1}/$numWarmup done. weight=${warmup.weight}, edges=${warmup.edges.size}, iterations=${warmup.iterations}")
      }

      var lastResult: Boruvka.BoruvkaResult = null
      val csvHeader = "library,graph,vertices,edges,cores,mst_weight,mst_edges,time_ms"
      val csvRows = scala.collection.mutable.ArrayBuffer.empty[String]

      for (i <- 0 until numRuns) {
        System.gc()
        printHeap(s"before run ${i + 1}", mem)

        val startTime = System.nanoTime()
        lastResult = Boruvka.run(graph, sc, debug, profile, mem, sparkOnly)
        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime) / 1e6

        printHeap(s"after run ${i + 1}", mem)

        println(s"Run ${i + 1}/$numRuns: ${"%.2f".format(timeMs)} ms")

        val timeMsStr = String.format(java.util.Locale.US, "%.2f", timeMs.asInstanceOf[java.lang.Double])

        val row =
          s"spark,$graphName,${meta.vertices},${meta.edges},$actualCores," +
            s"${lastResult.weight},${lastResult.edges.size},$timeMsStr"

        csvRows += row
      }

      if (checks) {
        val (resultComponents, coveredVertices) = timed("final result check") {
          val resultGraph = buildMstGraph(sc, lastResult.edges, meta.vertices)  // ← ИЗМЕНЕНО: передаём meta.vertices
          val components = resultGraph.connectedComponents().vertices.map(_._2).distinct().count()
          val covered = countCoveredVertices(lastResult.edges)

          (components, covered)
        }

        // ← ИЗМЕНЕНО: улучшенная проверка вершин
        val vertexCountCorrect = lastResult.edges.size == (meta.vertices - inputComponents)
        val componentCountCorrect = resultComponents == inputComponents
        val edgeCountCorrect = lastResult.edges.size == expectedResultEdges

        println()
        println("=== RESULT CHECK ===")
        println(s"input_vertices:          ${meta.vertices}")
        println(s"result_vertices_covered: $coveredVertices")
        println(s"vertex_count_correct:    $vertexCountCorrect")
        println(s"input_components:        $inputComponents")
        println(s"result_components:       $resultComponents")
        println(s"component_count_correct: $componentCountCorrect")
        println(s"expected_result_edges:   $expectedResultEdges")
        println(s"actual_result_edges:     ${lastResult.edges.size}")
        println(s"edge_count_correct:      $edgeCountCorrect")
      }

      println()
      println("=== RESULTS ===")
      println(s"graph:       $graphName")
      println(s"vertices:    ${meta.vertices}")
      println(s"edges:       ${meta.edges}")
      println(s"cores:       $actualCores")
      println(s"mst_weight:  ${lastResult.weight}")
      println(s"mst_edges:   ${lastResult.edges.size}")
      println(s"iterations:  ${lastResult.iterations}")

      println()
      println("=== CSV ===")
      println(csvHeader)
      csvRows.foreach(println)

      csvOutputPath.foreach { path =>
        val file = new File(path)
        val fileExists = file.exists()
        val writer = new PrintWriter(new java.io.FileWriter(file, true))
        try {
          if (!fileExists) writer.println(csvHeader)
          csvRows.foreach(writer.println)
        } finally {
          writer.close()
        }
        println(s"\nCSV appended to: $path")
      }

    } finally {
      printHeap("before spark stop", mem)
      timed("spark stop") {
        sc.stop()
      }
    }
  }

  /** Loads a Matrix Market (.mtx) file into a GraphX graph. */
  def loadMtxGraph(sc: SparkContext, path: String, meta: GraphMeta): Graph[Byte, Short] = {  // ← ИЗМЕНЕНО
    val lines = sc.textFile(path).filter(_.trim.nonEmpty).cache()

    val header = lines.first().toLowerCase
    val hasWeights = !header.contains("pattern")
    val isSymmetric = header.contains("symmetric")

    // ← ДОБАВЛЕНО: создаём ВСЕ вершины
    val vertices: RDD[(VertexId, Byte)] = sc.parallelize(
      (1L to meta.vertices).map(id => (id, 0.toByte))
    )

    val dataLines = lines
      .filter(line => !line.startsWith("%"))
      .zipWithIndex()
      .filter { case (_, idx) => idx > 0 }
      .map(_._1)

    val edges = dataLines.flatMap { line =>
      val p = line.trim.split("\\s+")
      if (p.length < 2) {
        Iterator.empty
      } else {
        val src = p(0).toLong
        val dst = p(1).toLong

        if (src == dst) {
          Iterator.empty
        } else {
          val w: Short =
            if (hasWeights && p.length >= 3) p(2).toShort
            else math.min(src, dst).toShort

          if (isSymmetric) {
            val (u, v) =
              if (src < dst) (src, dst)
              else (dst, src)
            Iterator.single(Edge(u, v, w))
          } else {
            Iterator.single(Edge(src, dst, w))
          }
        }
      }
    }.distinct()

    lines.unpersist()

    Graph(vertices, edges)  // ← ИЗМЕНЕНО: используем явные вершины
  }
}
