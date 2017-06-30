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
		
		val inputFile = "CA-GrQc.txt"

		//Inizializzo lo SparkContext
		val conf = new SparkConf().setAppName("Similarities")
		val sc = new SparkContext(conf)

		sc.setCheckpointDir("/tmp/spark-checkpoint")
		//Dall'input ricavo prima i collegamenti tra i vari autori, e poi da questi ottengo una lista dove ad ogni autore associo la lista degli altri autori con i quali ha collaborato
		val input = sc.textFile(inputFile)
		var edges = input.filter(l => l.length > 0 && l(0) != '#').map({l => val a = l.split("\\s+"); (a(0).toLong, a(1).toLong)})

		val startTime = System.nanoTime()
		//con questo passaggio evito di tralasciare i collegamenti mancanti, per esempio se esiste il collegamento 1 --> 2 ma non il viceversa, allora quest ultimo viene aggiunto
		edges = edges.map(x => (x._2, x._1)).union(edges).distinct 

		//Ricavo un RDD in cui ogni elemento è composto dall'id dell'autore e la sua lista di adiacenza
		val authors = edges.aggregateByKey(mutable.Set[Long]())(
				seqOp = (s, d) => s ++ mutable.Set(d),
				combOp = (s1, s2) => s1 ++ s2
			).map(v => (v._1, v._2.toArray)).distinct().sortByKey(true)

		authors.checkpoint() //eseguo il checkpointing dell'RDD authors
		val nAut = authors.count //authors Rdd evaluated by calling count() action
		
		//Eseguo il prodotto cartesiano sull'RDD authors così da ottenere ogni possibile coppia di autori e le loro corrispettive liste di adiacenza
		val cartesian_autohors: RDD[((Long, Array[Long]), (Long, Array[Long]))] = authors.cartesian(authors)

		cartesian_autohors.checkpoint() //eseguo il checkpointing dell'RDD cartesian_authors
		val nAut = authors.count //authors Rdd evaluated by calling count() action
				
		//Definisco i valori da passare alla funzione obtainSimilaritiesMatrix
		val default_jaccard:Double = nAut / (authors.map(x => x._2.length).sum).toDouble //oppure 1 / (authors.map(x => x._2.length).sum).toDouble???
		val default_preference: Double = 10.0
		val similarities_matrix = obtainSimilaritiesMatrix(cartesian_autohors, default_preference, default_jaccard)
		
		//Salvo la matrice di similarità su file
		val outFile = similarities_matrix.map(x => x._1 + "\t" + x._2 + "\t" + x._3)
		outFileS.saveAsTextFile("output/sim_nID-" + nAut) 

		val elapsedTime = System.nanoTime() - startTime
		println("Tempo stimato di esecuzione: " + (elapsedTime / 1000000000.0) + " secondi, n° similarities: " + aut.count)
		}


	def obtainSimilaritiesMatrix(authors: RDD[( (Long, Array[Long]), (Long, Array[Long]) )], default_preference: Double, default_jaccard: Double)  : RDD[(Long, Long, Double)]  = {
			
			val initial_similarities_matrix : RDD[(Long, Long, Double)] = authors.map{x =>
										var a: Double = (x._1._2.intersect(x._2._2).length);
										var b: Double = (x._1._2.union(x._2._2).length);
										var jaccard: Double = if(a != 0) math.log(a/b) else math.log(default_jaccard);
										var sim: Double = jaccard * default_preference;
										
										(x._1._1, x._2._1, sim)
										}

			//Ricavo i valori delle similarità per calcolare poi la preference
			val similarities_values = initial_similarities_matrix.filter(x => x._1 != x._2).map(x => x._3)
			val preference: Double = (similarities_values.max + similarities_values.min) / 2

			//Utilizzo la preference per aggiornare la initial_similarities_matrix, nella quale ci sono preferences errate
			val tmp1 = initial_similarities_matrix.filter(x => x._1 == x._2).map(x => (x._1, x._2, preference))
			val tmp2 = initial_similarities_matrix.filter(x => x._1 != x._2)
			
			//Il valore di ritorno
			tmp1.union(tmp2)
	}
}
