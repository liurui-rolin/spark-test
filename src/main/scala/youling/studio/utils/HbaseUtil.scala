/**
 *
 * com.momo.game.spark.realTimeStat.utils包引入声明：
 * 1.XXX
 * 2.XXX
 * @copyright 北京陌陌信息技术有限公司
 */
package youling.studio.utils

import java.util.ArrayList

import scala.collection.Iterator

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.log4j.Logger

/**
 *
 * 功能描述：存储结果到hbase
 * @author wgc 2016年4月26日下午6:18:08
 */
class HbaseUtil() {
  private val logger = Logger.getLogger(this.getClass())
  private val config = HBaseConfiguration.create()
  private val admin = new HBaseAdmin(config)
  val userTable = new HTable(config, "realtime_stat_user")
  val payTable = new HTable(config, "realtime_stat_pay")

  def insertInt(records: Iterator[(String, Int)], tab: HTable, colName: String) {
    val putList = new ArrayList[Put]()
    for ((keys, values) <- records) {
      val put = new Put(keys.getBytes())
      put.add("f1".getBytes(), colName.getBytes(), values.toString.getBytes())
      putList.add(put)
    }
    tab.put(putList)
    tab.flushCommits()
  }

  def insertTotalSet(timeStr: String, records: Map[String, Set[String]], tab: HTable, colName: String) {
    val putList = new ArrayList[Put]()
    for ((keys, values) <- records) {
      val put = new Put((keys + "@" + timeStr).getBytes())
      put.add("f1".getBytes(), colName.getBytes(), values.size.toString.getBytes())
      putList.add(put)
    }
    tab.put(putList)
    tab.flushCommits()
  }

  def insertTotalInt(timeStr: String, records: Map[String, Int], tab: HTable, colName: String) {
    val putList = new ArrayList[Put]()
    for ((keys, values) <- records) {
      val put = new Put((keys + "@" + timeStr).getBytes())
      put.add("f1".getBytes(), colName.getBytes(), values.toString.getBytes())
      putList.add(put)
    }
    tab.put(putList)
    tab.flushCommits()
  }

  def close() {
    admin.close()
  }

}
