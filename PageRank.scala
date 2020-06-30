import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    println("==>Initializing Spark Context")
    val conf = new SparkConf().setAppName("PageRank").setMaster("local[4]")
    conf.set("spark.testing.memory","2147480000")
    conf.set("spark.executor.memory","32g")
    conf.set("spark.driver.memory","32g")
    val sc = new SparkContext(conf)

    println("==>Load the edges as a graph")
    val graph = GraphLoader.edgeListFile(sc, "C:\\Users\\22859\\Desktop\\wiki.txt")
    println(" - Vertex num: " + graph.vertices.count())
    println(" - Edge num: " + graph.edges.count())

    println("==>Run PageRank")
    val ranks = graph.pageRank(0.0001).vertices

    println("==>Print the result")
    println(ranks.collect().mkString("\n"))

    println("==>Done")
  }
}