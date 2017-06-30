import java.util.concurrent.TimeUnit.NANOSECONDS
import java.text.{NumberFormat, DecimalFormat}
import scala.collection.mutable
import scala.math
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Sim extends Serializable{

	def main(args: Array[String]) {
		
		val inputFile = "prova.txt"

		//Inizializzo lo SparkContext
		val conf = new SparkConf().setAppName("Similarities")
		val sc = new SparkContext(conf)

		sc.setCheckpointDir("/tmp/spark-checkpoint")
		//Dall'input ricavo prima i collegamenti tra i vari autori, e poi da questi ottengo una lista dove ad ogni autore associo la lista degli altri autori con i quali ha collaborato
		val input = sc.textFile(inputFile)
		var edges = input.filter(l => l.length > 0 && l(0) != '#').map({l => val a = l.split("\\s+"); (a(0).toLong, a(1).toLong)})
println("INIZIO")
		val startTime = System.nanoTime()
		edges = edges.map(x => (x._2, x._1)).union(edges).distinct //con questo passaggio evito di tralasciare i collegamenti mancanti, per esempio se esiste il collegamento 1 --> 2 ma non il viceversa, allora quest ultimo viene aggiunto

		val authors = edges.aggregateByKey(mutable.Set[Long]())(
				seqOp = (s, d) => s ++ mutable.Set(d),
				combOp = (s1, s2) => s1 ++ s2
			).map(v => (v._1, v._2.toArray)).distinct().sortByKey(true)

		authors.checkpoint()
		val nAut = authors.count.toInt //authors Rdd evaluated by calling count() action
		val nSim_i = (nAut*(nAut-1) / 2)
		
		val cartesian_autohors: RDD[((Long, Array[Long]), (Long, Array[Long]))] = authors.cartesian(authors)

		
		val default_jaccard:Double = nAut / (authors.map(x => x._2.length).sum).toDouble //oppure 1 / (authors.map(x => x._2.length).sum).toDouble???
		val default_preference: Double = 10.0
		val similarities = obtainSimilaritiesMatrix(cartesian_autohors, default_preference, default_jaccard)
		
		//val out = similarities.filter(x => x._1 != x._2).map(x => (x._2, x._1, x._3)).union(similarities)
		val outFile = similarities.map(x => x._1 + "\t" + x._2 + "\t" + x._3)
		outFile.repartition(1).saveAsTextFile("output/sim_" + nAut)

		val elapsedTime = System.nanoTime() - startTime
		println("Tempo stimato di esecuzione: " + (elapsedTime / 1000000000.0) + " secondi, n° similarities: " + aut.count)
		}


	/*def calculateSimilarities(authors: RDD[(Long, Array[Long])], n: Double, s: Int)  : List[(Long, Long, Double)]  = {
			val formatter = new DecimalFormat("#0.000");
			var idx = 1
			val default_preference: Double = 10.0
			val default_jaccard:Double = 1 / (authors.map(x => x._2.length).sum).toDouble
			val a = authors.collect().toList
			val atr = a.tails.flatMap {
			  case (a1: Long, co1: Array[Long]) :: (rest : List[(Long, Array[Long])]) => rest.map { 
				x => 
					var a: Double = (co1.intersect(x._2).length);
					var b: Double = (co1.union(x._2).length);
					var jaccard: Double = if(a != 0) math.log(a/b) else math.log(default_jaccard);
					var sim: Double = jaccard * default_preference;

					

					if(idx % 2000 == 0) {
						val p: String = formatter.format( (idx.toDouble / s) * 100.0 )
						println(idx + ") : " + "A1: " + a1 + ", A2: " + x._1 + ", SIM: " + sim + " ----> " + p + " %");
						}
					idx = idx + 1;

					(a1, x._1, sim)
				
			  	}
			  case x => List()
			}
			val tmp = atr.union(atr.map(x => (x._2, x._1, x._3)))
			val simList = atr.map(x => x._3)
			val preference = (simList.max + simList.min) / 2
			val ris = tmp.union(a.map(x => (x._1, x._1, preference)))

			ris
	}*/


	def obtainSimilaritiesMatrix(authors: RDD[( (Long, Array[Long]), (Long, Array[Long]) )], default_preference: Double, default_jaccard: Double)  : RDD[(Long, Long, Double)]  = {
			
			val initial_similarities_matrix : RDD[(Long, Long, Double)] = authors.map{x =>
										var a: Double = (x._1._2.intersect(x._2._2).length);
										var b: Double = (x._1._2.union(x._2._2).length);
										var jaccard: Double = if(a != 0) math.log(a/b) else math.log(default_jaccard);
										var sim: Double = jaccard * default_preference;
										
										(x._1._1, x._2._1, sim)
										}

			val similarities_values = initial_similarities_matrix.filter(x => x._1 != x._2).map(x => x._3)
			val preference: Double = (similarities_values.max + similarities_values.min) / 2
			val tmp1 = initial_similarities_matrix.filter(x => x._1 == x._2).map(x => (x._1, x._2, preference))
			val tmp2 = initial_similarities_matrix.filter(x => x._1 != x._2)
			val similarities_matrix = tmp1.union(tmp2)

			similarities_matrix
	}
}
