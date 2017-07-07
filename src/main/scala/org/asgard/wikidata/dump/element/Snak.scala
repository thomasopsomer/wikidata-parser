package org.asgard.wikidata.dump.element

import scala.util.{Success, Try}

import org.json4s._
import org.json4s.jackson.JsonMethods._

/*
 * Snak are object of a given property. There can be to type:
 *  - wikibase-item: in this case the property is a relation to another
 *    wikidata entity
 *  - external-id: in this case the value is a string, for instance the quora topic id
 *    related to the wikidata item being processed
 */

class Snak(json: JValue){

  lazy val dataType: Option[String] = {
    Try((json \ "mainsnak" \ "datatype" ).asInstanceOf[JString].s) match{
      case Success(s) => Some(s)
      case _ => None
    }
  }

  lazy val argumentId: Option[String] = {
    dataType match {
      case Some(t) => {
        t match {
          case "wikibase-item" =>
            Try((json \ "mainsnak" \ "datavalue" \ "value" \ "id").asInstanceOf[JString].s) match {
              case Success(s) => Some(s)
              case _ => None
            }
          case "external-id" =>
            Try((json \ "mainsnak" \ "datavalue" \ "value").asInstanceOf[JString].s) match {
              case Success(s) => Some(s)
              case _ => None
            }
          case _ => None
        }
      }
    }
  }

}
