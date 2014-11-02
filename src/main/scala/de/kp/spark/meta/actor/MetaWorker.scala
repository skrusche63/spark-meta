package de.kp.spark.meta.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Meta project
* (https://github.com/skrusche63/spark-meta).
* 
* Spark-Meta is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Meta is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Meta. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.meta.model._
import de.kp.spark.meta.redis.RedisCache

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import scala.collection.mutable.ArrayBuffer

class MetaWorker extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ServiceRequest => {
       
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {

        req.service match {

	      case "association" => {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("group","string",req.data("group"))

            fields += new Field("item","integer",req.data("item"))
            
            RedisCache.addFields(req, new Fields(fields.toList))        
            new ServiceResponse("association","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
            
	      }
	      case "context" => {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            /*
             * ********************************************
             * 
             *  "uid" -> 123
             *  "names" -> "target,feature,feature,feature"
             *
             * ********************************************
             * 
             * It is important to have the names specified in the order
             * they are used (later) to retrieve the respective data
             */
            val names = req.data("names").split(",")
            for (name <- names) {
              fields += new Field(name,"double","")
            }
 
            RedisCache.addFields(req, new Fields(fields.toList))       
            new ServiceResponse("context","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	        
	      }
          case "decision" => {
       
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            /*
             * ********************************************
             * 
             *  "uid" -> 123
             *  "names" -> "target,feature,feature,feature"
             *  "types" -> "string,double,double,string"
             *
             * ********************************************
             * 
             * It is important to have the names specified in the order
             * they are used (later) to retrieve the respective data
             */
            val names = req.data("names").split(",")
            val types = req.data("types").split(",")
        
            val zip = names.zip(types)
        
            val target = zip.head
            if (target._2 != "string") throw new Exception("Target variable must be a String")
        
            fields += new Field(target._1,target._2,"")
        
            for (feature <- zip.tail) {
          
              if (feature._2 != "string" && feature._2 != "double") throw new Exception("A feature must either be a String or a Double.")          
              fields += new Field(feature._1, if (feature._2 == "string") "C" else "N","")
        
            }
 
            RedisCache.addFields(req, new Fields(fields.toList))        
            new ServiceResponse("decision","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
            
	      }
          case "intent" => {
	    
	        req.task match {	      
	          
	          case "register:loyalty" => {
        
                /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                fields += new Field("site","string",req.data("site"))
                fields += new Field("timestamp","long",req.data("timestamp"))

                fields += new Field("user","string",req.data("user"))
                fields += new Field("amount","float",req.data("amount"))
            
                RedisCache.addFields(req, new Fields(fields.toList))        
                new ServiceResponse("intent","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	            
	          }

	          case "register:purchase" => {
        
                /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                fields += new Field("site","string",req.data("site"))
                fields += new Field("timestamp","long",req.data("timestamp"))

                fields += new Field("user","string",req.data("user"))
                fields += new Field("amount","float",req.data("amount"))
            
                RedisCache.addFields(req, new Fields(fields.toList))        
                new ServiceResponse("intent","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	            
	          }
	      
	          case _ => throw new Exception("Unknown task.")
	      
	        }
      
          }
	      case "outlier" => {
	    
	        req.task match {
	          
	          case "register:features" => {
            
	            /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                /*
                 * ********************************************
                 * 
                 *  "uid" -> 123
                 *  "names" -> "target,feature,feature,feature"
                 *  "types" -> "string,double,double,double"
                 *
                 * ********************************************
                 * 
                 * It is important to have the names specified in the order
                 * they are used (later) to retrieve the respective data
                 */
                val names = req.data("names").split(",")
                val types = req.data("types").split(",")
        
                val zip = names.zip(types)
        
                val target = zip.head
                if (target._2 != "string") throw new Exception("Target variable must be a String")
        
                fields += new Field(target._1,target._2,"")
        
                for (feature <- zip.tail) {
          
                  if (feature._2 != "double") throw new Exception("A feature must be a Double.")          
                  fields += new Field(feature._1,"double","")
        
                }
 
                RedisCache.addFields(req, new Fields(fields.toList))        
                new ServiceResponse("outlier","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	            
	          }
	          case "register:sequences" => {
        
                /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                fields += new Field("site","string",req.data("site"))
                fields += new Field("timestamp","long",req.data("timestamp"))

                fields += new Field("user","string",req.data("user"))
                fields += new Field("group","string",req.data("group"))

                fields += new Field("item","integer",req.data("item"))
                fields += new Field("price","float",req.data("price"))
            
                RedisCache.addFields(req, new Fields(fields.toList))        
                new ServiceResponse("outlier","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	            
	          }
	      
	          case _ => throw new Exception("Unknown task.")
	    
	        }
	    
	      }
	      case "series" => {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("group","string",req.data("group"))

            fields += new Field("item","integer",req.data("item"))
        
            RedisCache.addFields(req, new Fields(fields.toList))        
            new ServiceResponse("series","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	      
	      }
	      case "similarity" => {
	    
	        req.task match {
	          
	          case "register:features" =>{
        
                /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                /*
                 * ********************************************
                 * 
                 *  "uid" -> 123
                 *  "names" -> "target,feature,feature,feature"
                 *  "types" -> "string,double,double,double"
                 *
                 * ********************************************
                 * 
                 * It is important to have the names specified in the order
                 * they are used (later) to retrieve the respective data
                 */
                val names = req.data("names").split(",")
                val types = req.data("types").split(",")
        
                val zip = names.zip(types)
        
                val target = zip.head
                if (target._2 != "string") throw new Exception("Target variable must be a String")
        
                fields += new Field(target._1,target._2,"")
        
                for (feature <- zip.tail) {
          
                  if (feature._2 != "double") throw new Exception("A feature must be a Double.")          
                  fields += new Field(feature._1,"double","")
        
                }
 
                RedisCache.addFields(req, new Fields(fields.toList))       
                new ServiceResponse("similarity","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
	            
	          }	
  	          case "register:sequences" => {
        
                /* Unpack fields from request and register in Redis instance */
                val fields = ArrayBuffer.empty[Field]

                fields += new Field("site","string",req.data("site"))
                fields += new Field("timestamp","long",req.data("timestamp"))

                fields += new Field("user","string",req.data("user"))
                fields += new Field("group","string",req.data("group"))

                fields += new Field("item","integer",req.data("item"))
            
                RedisCache.addFields(req, new Fields(fields.toList))        
                new ServiceResponse("similarity","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
  	            
  	          }
	      
	          case _ => throw new Exception("Unknown task.")
	      
	        }
	    
	      }
	      case "social" => {
	        /* Not implemented yet */
	      }
	      case "text" => {
	        /* Not implemented yet */
	      }

	      case _ => throw new Exception("Unknown service.")
 	    
	    }
       
        new ServiceResponse("association","register",Map("uid"-> uid),ResponseStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)
  
    }
    
  }
 
  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,ResponseStatus.FAILURE)	
      
    } else {
      val data = Map("uid" -> req.data("uid"), "message" -> message)
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
    
    }
    
  }
  
}