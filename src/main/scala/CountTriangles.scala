import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx
import org.apache.spark.graphx._

object CountTriangles {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Count Triangles")
    val sc = new SparkContext(conf)

    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, "sample.in", true).partitionBy(PartitionStrategy.RandomVertexCut)

    // zwraca tablice 2 elemetowych tupli (wierzcholek, ilosc_trojkatow_do_ktorych_nalezy
    val triCounts = graph.triangleCount().vertices.collect()
	
	var sum = 0;
	
	// zliczamy sumę wystąpień w trójkątach po wszystkich wierzchołkach
	triCounts.foreach(n => sum += n._2)
	
	// każdy z trójkątów policzyliśmy 3 razy
	sum /= 3
	
	println("Ilość trójkątów: " + sum)
  }
}
