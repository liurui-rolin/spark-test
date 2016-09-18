package youling.studio.utils

/**
 * Created by rolin on 16/7/27.
 * redis dao 类
 */

import redis.clients.jedis.Jedis

class RedisDao(val clients : Jedis) extends Serializable{
  def incrby(key:String,num :Int): Unit ={
    clients.incrBy(key,num)
    clients.expire(key,60*60*24*2)
  }
}

object RedisDao {
  val clients = new Jedis("redis_node_6320",6320)
  val redisDao = new RedisDao(clients)
  def getRedis(): RedisDao ={
    redisDao
  }
}
