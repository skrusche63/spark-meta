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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.meta.actor.MetaMaster

object MetaService {

  def main(args: Array[String]) {
    
    val name:String = "meta-server"
    val conf:String = "server.conf"

    val server = new MetaService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class MetaService(conf:String, name:String) {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)

  val master = system.actorOf(Props[MetaMaster], name="meta-master")

  def shutdown = system.shutdown()
  
}