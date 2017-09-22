package org.asgard.wikidata.dump.element

import scala.util.{Success, Try}

import org.json4s._
import org.json4s.jackson.JsonMethods._

/*
* Represents a Topic in Wikidata
* They are addressed as Items within the wikidata Jargon
* */
class Item(parsedJson: JValue) extends DumpElement {

  implicit val formats = DefaultFormats

  def getAliases(language: String): Option[List[String]] = {
    Try(parsedJson \ "aliases" \ language \ "value" \\ classOf[JString]) match{
      case Success(s) => Some(s)
      case _ => None
    }
  }

  def getType: Option[String]={
    Try((parsedJson \ "type" ).asInstanceOf[JString].s) match{
      case Success(s) => Some(s)
      case _ => None
    }
  }

  def getId: Option[String]={
    Try((parsedJson \ "id").asInstanceOf[JString].s) match{
      case Success(s) => Some(s)
      case _ => None
    }
  }

  def getClaims:List[Claim] ={
    Try((parsedJson \ "claims").asInstanceOf[JObject]) match{
      case Success(claims) => {
        claims.obj.map{
          case (relationshipType:String, claim:JValue) =>
              new Claim(claim.asInstanceOf[JArray], relationshipType)
        }
      }
      case _ =>  List[Claim]()
    }
  }

  def getWikipediaReference(language:String):Option[String] = {
    val languageKey = language + "wiki"
    Try((parsedJson \ "sitelinks" \ languageKey \ "title").asInstanceOf[JString].s) match{
        case Success(s) => Some(s.replace(" ", "_"))
        case _ => None
    }
  }

  def getDescription(language: String): Option[String] = {
    Try((parsedJson \ "descriptions" \ language \ "value").asInstanceOf[JString].s) match{
      case Success(s) => Some(s)
      case _ => None
    }
  }

  /*
  * Returns a list of Tuples containing:
  *     (RelationshipTypePid, ArgumentQId)
  * */
  def getRelationshipTuples: List[(String, String)] = {
     getClaims.flatMap{
      claim =>
        claim.argumentQids.map{
           qid =>
             (claim.relationshipType, qid)
        }
    }
  }



}

