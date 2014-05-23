package scalding.examples

import com.twitter.scalding._
import java.net.URL
//import scala.util.matching.Regex

class WordCountJob(args : Args) extends Job(args) {

  //val myregex = new Regex("""(http|https)://([^.]+[w|1]\.|)([^.]+\.[^/]+)/|.+""")
  TextLine( args("input") )
    //.map('line,'word){line : String => myregex.findAllIn(line).matchData.toList(0).group(3)}
    .flatMap('line -> 'word){
    line: String => extractDomain(line)
  }
    .groupBy('word) { _.size }
    .filter('size){size: Int => size > 4}
    .write( Tsv( args("output") ) )

  def extractDomain(text: String) = Array[String]
  {
    val hostTopLevel = new URL(text).getHost.split("\\.")
    (hostTopLevel.reverse(1)+"."+hostTopLevel.reverse(0)).toString
  }
}

object WordCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scalding.examples.WordCountJob",
    "--input", "/home/kumaran/indixAssignments/scalding_examples/tmp/sampleurls.txt",
    "--output", "/home/kumaran/indixAssignments/scalding_examples/tmp/wordcount.txt",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}