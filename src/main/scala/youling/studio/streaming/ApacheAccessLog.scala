package youling.studio.streaming

case class ApacheAccessLog(ipAddress: String, clientIdentd: String,
  userId: String, dateTime: String, method: String,
  endpoint: String, protocol: String,
  responseCode: Int, contentSize: Long,
  referer: String, agent: String) {
  def getIpAddress(): String ={
    this.ipAddress
  }
}

object ApacheAccessLog {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w\d:\/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)"""".r
  def parseLogLine(log: String): ApacheAccessLog = {
    val res = PATTERN.findFirstMatchIn(log)
    if (res.isEmpty) {
      ApacheAccessLog("", "", "", "","", "", "", 0, 0, "", "")
    }
    else {
      val m = res.get
      val contentSizeSafe : Long = if (m.group(9) == "-") 0 else m.group(9).toLong
      val formattedEndpoint : String = (if (m.group(6).charAt(m.group(6).length-1).toString == "/") m.group(6) else m.group(6).concat("/"))

      ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), formattedEndpoint, m.group(7), m.group(8).toInt, contentSizeSafe, m.group(10), m.group(11))
    }
  }
}