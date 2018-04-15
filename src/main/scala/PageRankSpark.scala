import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.immutable.List
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Ordering

/**
 * PageRank in Spark.
 * read in the bz2-compressed input, call the input parser on each line of this input to create a graph,
 * runs 10 iterations of PageRank on the graph (deals with dangling nodes), and outputs the top-100 pages 
 * with the highest PageRank and their PageRank values, in decreasing order of PageRank.
 */
object PageRankSpark {
  
	def main(args : Array[String]) {
		
		// constants
		val loop: Int = 10
		val alpha: Double = 0.15
		
    try {
    	
      val sparkContext = new SparkContext( new SparkConf().setAppName("PageRank").setMaster("local[4]"))
      //val sparkContext = new SparkContext( new SparkConf())
      
      // parsing file through MyParser java job
      val inputGraph = sparkContext.textFile(args(0), sparkContext.defaultParallelism)
        // iterate over every line
        .map( line => MyParser.readXML(line) )
        // removes incorrect values due to tilde or other bad formatting
        .filter( line => !line.contains("bad") )
        // page and link names separator
        .map( line => line.split("dummy") )
        .map( line => if (line.length == 1) {
        	// assign an empty list if outlinks dont exist 
          ( line(0), List() )
        } else { // splitting the node
          ( line(0), line(1).split("~").toList )
        })

      // cache the RDD in memory (increase perf, faster access)
      inputGraph.persist()
      
      // find the dangling nodes then combine the list of outlinks (the unique node with links)
      var finalLinks = inputGraph.values
        .flatMap { 
      	  node => node 
      	}
        // pageName -> key
        .keyBy( node => node )
        // map the key to an empty list
        .map( line => (line._1, List[String]()) )
          // union of pageName and outLinks to find all nodes
          .union(inputGraph)
          // the dangling node with an outlink is an empty list (reduce by key)
          .reduceByKey( (value1, value2) => value1.++(value2) )
      
      val numberOfNodes = finalLinks.count()
      
      // initialize pageranks with 1/k
      val initialPageRank: Double = 1.0/numberOfNodes
      
      // give the initial page rank to all the nodes
      var uniqueNodeWithPageRank = finalLinks.keys
      .map(line => (line, initialPageRank))
      
      // pageranks for 10 iterations
      for (i <- 1 to loop) {
        try {
        	// the accumulator to account for dangling factor
          var danglingFactor = sparkContext.accumulator(0.0)
          
          // do a full join by key to remove unnecessary data
          var pageRankSetValues = finalLinks.join(uniqueNodeWithPageRank)
            .values
            .flatMap {
              case (links, pageRank) =>
                val size = links.size
                if (size == 0) {
                	// update the dangling factor
                	// assign an empty list for a dangling node
                  danglingFactor += pageRank
                  List()
                } else {
                	// if not dangling node, update the page rank values
                  links.map(url => (url, pageRank / size))
                }
            }
            // combine the page rank contribution of outlinks to node with reduce by key
            .reduceByKey(_ + _)
            
            pageRankSetValues.first()
            
            val danglingFactorSum: Double = danglingFactor.value
            
            // get the node with no inlinks
            uniqueNodeWithPageRank = uniqueNodeWithPageRank.subtractByKey(pageRankSetValues)
               // setting value of page rank for nodes without inlinks to 0
              .map(rec => (rec._1 ,0.0))
              // union with page rank with links
              .union(pageRankSetValues)
              .mapValues
              
            // update the values of page rank for all outlinks
            [Double](pageRankList => alpha * initialPageRank + (1-alpha) * (danglingFactorSum / numberOfNodes + pageRankList))
            
        } catch {
          case e: Exception => println(s"Unknown exception 2: $e")
        }
      } // end for loop
        
      // output the top-100 pages with the highest PageRank and their PageRank values, in decreasing order of PageRank
      // sort and take the top 100
      var top100 = uniqueNodeWithPageRank
        .takeOrdered(100)( Ordering[Double]
          .reverse.on { line => line._2 } )

      // save the top 100
      sparkContext.parallelize(top100).saveAsTextFile(args(1))
      
    } // end of try
    catch {
      case ex: Exception => println(s"Unknown exception 1: $ex")
    }
  }
}
