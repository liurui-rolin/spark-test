package youling.studio.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 *
 * 功能描述：时间工具类
 * @author wgc 2016年5月5日下午4:28:00
 */
object TimeUtil {
  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")

  // 获取60s前的分钟
  private def getMinBefore60s(): String = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.SECOND, -60)
    dateFormat.format(cal.getTime())
  }

  // 获取分钟十位数
  def getMin(timeStr: String): String = {
    (timeStr.replaceAll("[\\s-:]", "").substring(0, 12).toLong / 10 * 10).toString
  }

  // 获取60s前的分钟十位数
  def get10MinBefore60s(): String = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.SECOND, -60)
    (dateFormat.format(cal.getTime()).toLong / 10 * 10).toString
  }

  def isZeroPoint(): Boolean = {
    (getMinBefore60s().toLong % 10000).equals(0l)
  }

}