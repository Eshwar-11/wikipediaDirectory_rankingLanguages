package wikipedia

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.TrueLiteral

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Wikipedia")

  val sc: SparkContext = new SparkContext(conf)

  // TASK 1 //////////////////////////////////////////////////////////////////////
  /* val WikipediaData = sc.textFile("data/wikipedia.dat")
  val articlesinData = WikipediaData.map(i => i.split(","))
  println(articlesinData) */

  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath)
                                          .map(x=>WikipediaData.parse(x))
  //val contents = input.flatM

  // TASK 2 //////////////////////////////////////////////////////////////////////

  // TASK 2: attempt #1 ----------------------------------------------------------

  /** Returns the number of articles in which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    /* def f(acc:Int,wa: WikipediaArticle): Int=
    if wa.mentionsLanguage(lang) then acc+1 
    else acc
    rdd.aggregate(0)(f, _+_) */

    rdd.aggregate(0)((mentions,article) => mentions + (if(article.mentionsLanguage(lang))then 1 else 0),_+_)
    //rdd.filter(articles=>articles.mentionsLanguage(lang)).count().toInt
  }


  /* Uses `occurrencesOfLang` to compute the ranking of the languages
   * (`val langs`) by determining the number of Wikipedia articles that
   * mention each language at least once.
   *
   * IMPORTANT: The result is sorted by number of occurrences, in descending order.
   * 
   * 
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
  // TASK 2: attempt #2 ----------------------------------------------------------
   langs.map(lang=>(lang,occurrencesOfLang(lang,rdd))).sortBy(_._2)
                                                      .reverse
  } 

  /* Computes an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] ={

    // collection of all pairs (l, wa), where l is a language and wa is a Wikipedia article.

    val pairs: RDD[(String,WikipediaArticle)] = rdd.flatMap(articles=> langs.map(l=> (l,articles)))

    // collection of all pairs (l, wa) where wa is an article that mentions language l.
    val mentionedPairs: RDD[(String,WikipediaArticle)] = rdd.flatMap(articles => 
                                                                                  langs.filter(l=>articles.mentionsLanguage(l))
                                                                                       .map(lnA=>(lnA,articles)))
      /* rdd.flatMap(wikiArticles => langs.map(l=> if(wikiArticles.mentionsLanguage(l))(l,wikiArticles)else (null,null)))
                                                            .filter(_!=null) */

     // Hint: use `filter` and `mentionsLanguage`
    mentionedPairs.groupByKey // <<<<  replace ??? with the expression you want this function to return
  }
  /* Computes the language ranking using the inverted index.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(temp=>temp.size).sortBy(_._2)
                                    .collect()
                                    .toList
                                    .reverse
  }


  // TASK 2: attempt #3 ----------------------------------------------------------

  /* Creates a list of (lang, integer) pairs containing one pair (l, 1) for each Wikipedia
   * article in which language l occurs.
   */
  def zipLangWithPoint(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Int)] = {
    rdd.flatMap(zippedArticles => 
                                  langs.filter(l=>zippedArticles.mentionsLanguage(l))
                                       .map(ln1=>(ln1,1)))
    //rdd.flatMap(zippedArticles => langs.map(l=> (l,if(zippedArticles.mentionsLanguage(l))1 else 0)))

  }

  /* Uses `reduceByKey` to compute the index and the ranking simultaneously.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = { 
    
    zipLangWithPoint(langs,rdd).reduceByKey(_+_)
                               .sortBy(_._2)
                               .collect()
                               .toList
                               .reverse

  }



  def main(args: Array[String]): Unit =

    Logger.getLogger("org").setLevel(Level.ERROR)

    val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

    //println("Hello world! The u.data file has " + numLines + " lines.")

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] =
      timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
    println(makeIndex(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)]
      = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)]
      = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()


  // Do not edit `timing` or `timed`.
  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

}

