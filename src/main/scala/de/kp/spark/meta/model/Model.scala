package de.kp.spark.meta.model

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)
/*
 * The Field and Fields classes are used to specify the fields with
 * respect to the data source provided, that have to be mapped onto
 * site,timestamp,user,group,item
 */
case class Field(
  name:String,datatype:String,value:String
)
case class Fields(items:List[Field])

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializeFields(fields:Fields):String = write(fields) 
  def deserializeFields(fields:String):Fields = read[Fields](fields)

  def serializeResponse(response:ServiceResponse):String = write(response)  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  
}


object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}
