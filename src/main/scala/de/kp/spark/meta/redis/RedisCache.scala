package de.kp.spark.meta.redis
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

import java.util.Date

import de.kp.spark.meta.model._
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()

  def addFields(req:ServiceRequest,fields:Fields) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "fields:" + req.service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeFields(fields)
    
    client.zadd(k,timestamp,v)
    
  }

}