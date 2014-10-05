package de.kp.spark.meta.redis

import redis.clients.jedis.Jedis
import de.kp.spark.meta.Configuration

object RedisClient {

  def apply():Jedis = {

    val (host,port) = Configuration.redis
    new Jedis(host,port.toInt)
    
  }
  
}