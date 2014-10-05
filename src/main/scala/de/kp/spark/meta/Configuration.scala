package de.kp.spark.meta
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

import com.typesafe.config.ConfigFactory
object Configuration {

    /* Load configuration for router */
  val path = "application.conf"
  val config = ConfigFactory.load(path)

  def actor():Int = {
  
    val cfg = config.getConfig("actor")
    val timeout = cfg.getInt("timeout")
    
    timeout
    
  }
  
  def redis():(String,String) = {
  
    val cfg = config.getConfig("redis")
    
    val host = cfg.getString("host")
    val port = cfg.getString("port")
    
    (host,port)
    
  }
  
  def router():(Int,Int,Int) = {
  
    val cfg = config.getConfig("router")
  
    val time    = cfg.getInt("time")
    val retries = cfg.getInt("retries")  
    val workers = cfg.getInt("workers")
    
    (time,retries,workers)

  }
  
}