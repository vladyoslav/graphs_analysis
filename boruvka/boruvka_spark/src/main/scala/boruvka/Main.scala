package boruvka

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.io.Source
import java.io.{File, PrintWriter}

object Main {

  /** Parses command-line arguments of the form --key=value into a Map. */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      if (arg.startsWith("--") && arg.contains("=")) {
        val parts = arg.drop(2).split("=", 2)
        Some(parts(0) -> parts(1))
      } else {
        None
      }
    }.toMap
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    // Required parameter
    val inputPath = params.getOrElse("graph", {
      System.err.println("Error: --graph is required.")
      System.err.println("Usage: BoruvkaMST --graph=<path> [--csv=<path>] [--runs=<n>] [--cores=<n>]")
      System.err.println("  --graph   path to .mtx file (required)")
      System.err.println("  --csv     path to save CSV results (optional)")
      System.err.println("  --runs    number of benchmark runs (default: 5)")
      System.err.println("  --cores   number of CPU cores (default: * = all)")
      System.exit(1)
      "" // unreachable, but compiler needs a return value
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

    // Optional parameters
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

    val conf = new SparkConf()
      .setAppName("BoruvkaMST")
      .setMaster(s"local[$numCores]")
    val sc = new SparkContext(conf)
    val actualCores = sc.defaultParallelism
    sc.setLogLevel("ERROR")

    try {
      // Load and cache graph before timing
      val graph = loadMtxGraph(sc, inputPath)
      graph.cache()
      val numVertices = graph.vertices.count()
      val numEdges = graph.edges.count()

      // Warmup run (JIT compilation)
      val warmup = Boruvka.run(graph, sc)
      println(s"Warmup done. MST weight: ${warmup.weight}")

      // Benchmark runs
      val times = new Array[Double](numRuns)
      var lastResult: Boruvka.BoruvkaResult = null

      for (i <- 0 until numRuns) {
        System.gc()
        val startTime = System.nanoTime()
        lastResult = Boruvka.run(graph, sc)
        val endTime = System.nanoTime()
        times(i) = (endTime - startTime) / 1e6
        println(s"  Run ${i + 1}/$numRuns: ${"%.2f".format(times(i))} ms")
      }

      // Statistics
      val meanTime = times.sum / times.length
      val stdDev = math.sqrt(
        times.map(t => (t - meanTime) * (t - meanTime)).sum / times.length
      )
      val runtime = Runtime.getRuntime
      val usedMemoryMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0)
      val graphName = inputPath.split("/").last

      // Human-readable output
      println()
      println("=== RESULTS ===")
      println(s"graph:       $graphName")
      println(s"vertices:    $numVertices")
      println(s"edges:       $numEdges")
      println(s"cores:       $actualCores")
      println(s"runs:        $numRuns")
      println(s"mst_weight:  ${lastResult.weight}")
      println(s"mst_edges:   ${lastResult.edges.size}")
      println(s"iterations:  ${lastResult.iterations}")
      println(s"mean_time:   ${"%.2f".format(meanTime)} ms")
      println(s"std_dev:     ${"%.2f".format(stdDev)} ms")
      println(s"memory_mb:   ${"%.1f".format(usedMemoryMb)}")

      // CSV output
      val csvHeader = "library,graph,vertices,edges,cores,runs,mst_weight,mst_edges,iterations,mean_time_ms,std_dev_ms,memory_mb"
      val csvRow = s"spark,$graphName,$numVertices,$numEdges,$actualCores,$numRuns," +
        s"${lastResult.weight},${lastResult.edges.size},${lastResult.iterations}," +
        s"${"%.2f".format(meanTime)},${"%.2f".format(stdDev)},${"%.1f".format(usedMemoryMb)}"

      // Print CSV to stdout
      println()
      println("=== CSV ===")
      println(csvHeader)
      println(csvRow)

      // Save CSV to file if path provided
      csvOutputPath.foreach { path =>
        val file = new File(path)
        val fileExists = file.exists()
        val writer = new PrintWriter(new java.io.FileWriter(file, true))
        try {
          if (!fileExists) writer.println(csvHeader)
          writer.println(csvRow)
        } finally {
          writer.close()
        }
        println(s"\nCSV appended to: $path")
      }

    } finally {
      sc.stop()
    }
  }

  /** Loads a Matrix Market (.mtx) file into a GraphX graph. */
  def loadMtxGraph(sc: SparkContext, path: String): Graph[Long, Double] = {
    val allLines = Source.fromFile(path).getLines().toList
    val header = allLines.head.toLowerCase
    val hasWeights = !header.contains("pattern")
    val isSymmetric = header.contains("symmetric")

    val dataLines = allLines
      .filter(!_.startsWith("%"))
      .drop(1)

    val edges = dataLines.flatMap { line =>
      val p = line.trim.split("\\s+")
      if (p.length < 2) Nil
      else {
        val src = p(0).toLong
        val dst = p(1).toLong
        if (src == dst) Nil
        else {
          val w = if (hasWeights && p.length >= 3) p(2).toDouble
                  else {
                    val lo = math.min(src, dst)
                    val hi = math.max(src, dst)
                    ((lo * 31 + hi * 17) % 1000 + 1).toDouble
                  }
          if (isSymmetric) List(Edge(src, dst, w), Edge(dst, src, w))
          else List(Edge(src, dst, w))
        }
      }
    }

    Graph.fromEdges(sc.parallelize(edges), 0L)
  }
}
