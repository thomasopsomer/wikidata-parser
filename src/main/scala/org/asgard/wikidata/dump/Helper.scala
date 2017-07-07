package org.asgard.wikidata.dump

/*
* Reads a nodes.csv.
* Returns a Mapping from Qids to LineIds( In which line is the Qid in node.csv)
* */
object Helper {

  /*
   * loadIds expect txt file with one entity / propety Id per line
   */
  def loadIds(idsPath: String): Set[String] = {
    scala.io.Source.fromFile(idsPath).getLines().toSet[String]
  }

  def loadQidToLine(pathToMapping:String) = {
    scala.io.Source.fromFile(pathToMapping).getLines().zipWithIndex.map{
      case (l:String, index:Int) =>
        (l.trim().split("\t")(0), index.toString)
    }.toMap
  }

  def loadQidToMid(pathToMapping:String) = {
    scala.io.Source.fromFile(pathToMapping).getLines().zipWithIndex.map{
      case (l:String, index:Int) =>
        val qid = l.trim().split("\t")(0)
        val mid = l.trim().split("\t")(2)
        (qid,mid)
    }.toMap
  }
}