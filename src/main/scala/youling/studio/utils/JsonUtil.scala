package youling.studio.utils

import org.apache.log4j.Logger
import org.json4s.JValue
import org.json4s.native.JsonMethods.parse
import org.json4s.string2JsonInput

/**
 *
 * 功能描述：Json解析工具
 * @author wgc 2016年4月22日下午5:07:07
 */
object JsonUtil {

  private val logger = Logger.getLogger(JsonUtil.getClass())

  def jsonParse(jsonStr: String): JValue = {
    try {
      val jsonObj = parse(jsonStr)
      jsonObj
    } catch {
      case ex: Exception =>
        logger.error("json解析失败", ex)
        return parse(""" { "error":"ParseException" } """)
    }
  }

}
