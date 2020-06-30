import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.GraphGenerators

object SSSP {
  def main(args: Array[String]): Unit = {
    println("==>Initializing Spark Context")
    val conf = new SparkConf().setAppName("SSSP").setMaster("local[4]")
    conf.set("spark.testing.memory","2147480000")
    conf.set("spark.executor.memory","32g")
    conf.set("spark.driver.memory","32g")
    val sc = new SparkContext(conf)

    println("==>Load the edges as a graph")
    val graph = GraphLoader.edgeListFile(sc,"C:\\Users\\22859\\Desktop\\google.txt")
    println(" - Vertex num: " + graph.vertices.count())
    println(" - Edge num: " + graph.edges.count())

    println("==>Initializing SSSP table")
    val sourceId: VertexId = 25602 // Set the source vertex
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    println("==>Run SSSP")
    val sssp = initialGraph.pregel(Double.PositiveInfinity) (
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    println("==>Done")
    println(sssp.vertices.take(7115).mkString("\n"))

    println("==>Done")
  }
}