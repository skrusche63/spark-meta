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

import de.kp.spark.meta.redis.RedisCache

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import scala.xml._

class MetaWorker extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:String => {
      
      val origin = sender    
      
      try {
        
        val root = XML.load(req)
        val uid = (root \ "@uid").toString
        
        RedisCache.addMeta(uid, req)
        origin ! "Metadata successfully registered."
        
      } catch {
        case e:Exception => origin ! e.getMessage()
      }
    }
    
    case _ => {}
  
  }
  
}