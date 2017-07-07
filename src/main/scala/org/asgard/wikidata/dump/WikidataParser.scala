package org.asgard.wikidata.dump

/**
  * Created by thomasopsomer on 07/07/2017.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.asgard.wikidata.dump.element.{DumpElement, Item, Property}
import scopt.OptionParser


case class WikidataItem(
                         qid: String,
                         props: List[Prop],
                         wikiTitle: String,
                         aliases: String)


case class Prop(pId: String, value: String)


object WikidataParser {

  case class Params(
                     inputPath: String = null,
                     outputPath: String = null,
                     numPartitions: Option[Int] = None,
                     test: Boolean = false,
                     itemsPath: Option[String] = None,
                     propsPath: Option[String] = None
                   )

  def preprocessDump(originalDump: RDD[String]): RDD[String] = {
    originalDump
      .filter(line => line != "[" && line != "]")
      .map { line => if (line.last == ',') line.substring(0, line.length - 1) else line }
  }

  def run(params: Params): Unit = {

    // init spark context
    val conf = new SparkConf()
      .setAppName("WikidumpParser")
      .set("spark.driver.allowMultipleContexts", "true")

    // val sc = new SparkContext(conf)
    val sc = SparkContext.getOrCreate(conf)
    sc.hadoopConfiguration.set("fs.s3a.connection.timeout", "500000")

    // sqlContext and implicits for dataframe
    val hc = new SQLContext(sc)
    import hc.implicits._

    // Load wikidump as rdd
    val originalDump = sc.textFile(params.inputPath)
    // preprocess dump to have a valid json per line
    var dump = preprocessDump(originalDump)

    // if test mode just process the first 100 lines
    if (params.test) {
      dump = sc.parallelize(dump.take(100))
    }

    // load list of entity ids and property ids to keep
    val QIds = params.itemsPath match {
      case Some(path) => Helper.loadIds(path)
      case _ => Set[String]()
    }
    val PIds = params.propsPath match {
      case Some(path) => Helper.loadIds(path)
      case _ => Set[String]()
    }

    // parse wikidump and transform in dataframe
    val df = dump.map(DumpElement.parseElement)
        .filter(x => x != null)
        .filter(x => QIds.contains(x.getId.orNull) || QIds.isEmpty)
        .flatMap {
          case item: Item => Some(WikidataItem(
            qid = item.getId.orNull,
            props = item.getRelationshipTuples.collect {
              case x if PIds.contains(x._1) || PIds.isEmpty => Prop(x._1, x._2)
            },
            wikiTitle = item.getWikipediaReference("en").orNull,
            aliases = item.getAliases("en").orNull))
          case prop: Property => None
        }
      .toDF()

    // save in parquet
    df.write.parquet(params.outputPath)

  }

  def main(args: Array[String]): Unit = {

    // Argument parser
    val parser = new OptionParser[Params]("SparkPdfParser") {
      head("Spark Application that parse archive of uspto patent in a folder and save it to a parquet file")

      opt[String]("inputPath").required()
        .text("path to the wikidata dump in json format (gz or bz2)")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("outputPath").required()
        .text("path to output parquet file")
        .action((x, c) => c.copy(outputPath = x))

      opt[Int]("numPartitions")
        .text("Number of partitions of rdd to process")
        .action((x, c) => c.copy(numPartitions = Some(x)))

      opt[Unit]("test")
        .text("Flag to test the software, process only 2 patent archive")
        .action((_, c) => c.copy(test = true))

      opt[String]("idsPath")
        .text("Path to txt file with list of entity ids to keep. If no path, we keep all wikidata entities")
        .action((x, c) => c.copy(itemsPath = Some(x)))

      opt[String]("propsPath")
        .text("Path to txt file with list of properties ids to keep. If no path, we keep all properties for each entity")
        .action((x, c) => c.copy(propsPath = Some(x)))

    }
    // parser.parse returns Option[C]
    parser.parse(args, Params()) match {
      case Some(params) => run(params)
      case None =>
        parser.showUsageAsError
        sys.exit(1)
    }
  }
}
