package youling.studio.ad

import org.json4s.JValue
import youling.studio.utils.TimeUtil

/**
 * Created by rolin on 16/7/26.
 *
 * 存储广告的json数据映射成的表
 * time,query_type,os,source_type,appid,source,active,did,user_type,gameuid,ip
 */
case class AdTableSchema(time: String,query_type:String,os:String,
  source_type:String,appid:String,source:String,
  active:String,did:String,user_type:String,
  gameuid:String,ip:String) {
}

object AdTableSchema {
  //转化方法
  def parseLogLine(log: JValue): AdTableSchema = {
    val time = TimeUtil.getMin((log \ "payload" \ "time").values.toString)
    AdTableSchema(time,(log \ "payload" \ "query_type").values.toString,
      (log \ "payload" \ "os").values.toString,(log \ "payload" \ "source_type").values.toString,
      (log \ "payload" \ "appid").values.toString,(log \ "payload" \ "source").values.toString,
      (log \ "payload" \ "active").values.toString,(log \ "payload" \ "did").values.toString,
      (log \ "payload" \ "user_type").values.toString,(log \ "payload" \ "gameuid").values.toString,
      (log \ "payload" \ "ip").values.toString)
  }
}
