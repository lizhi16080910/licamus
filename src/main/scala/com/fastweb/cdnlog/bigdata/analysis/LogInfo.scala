package com.fastweb.cdnlog.bigdata.analysis

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.io.Source

case class Record(cdnlog: String)
case class LogInfo(machine: String, ip: Long, isp: String, prv: String, domain: String, url: String, suffix: String, status: String,
                   cs: Long, refer: String, ua: String, es: Double, hit: String, timestmp: String, dd: Int, userid: String, rk: String, 
                   up: String, Fa: String, Fv: String, platform: String, node: String)

case class CsCountLog(domain: String,isp: String, prv:String,area_pid:String,area_cid:String,cs: Long, 
                      statusCode: String, userid: String,timestmp: Long,platformName:String,popID:Int,node:Long,es:Double)

case class BossLogInfo(machineId: String, timestamp: String, isp: String, prv: String, domain: String,
                       statusCode: String, hitType: Int, ua: String, speedInterval: String,
                       speedSize: Double, es: Double, flowSize: Long, reqNum: Long, userid: String,
                       zeroSpeedReqNum: Long, platform: String, popID: Int, machineIsp: String,
                       machinePrv: String, machineIspCode: Int, machinePrvCode: Int)

object LogInfo {
  private[this] val log = Logger.getLogger(this.getClass)

  val HIT = 0
  val PARENT_HIT = 1
  val SOURCE = 2
  val PARENT_MISS = 3
  //val PARENT_MISS  =  1
  val MISS_OTHER = 4
  val NO_HIT = 8
  val timeoutInterval = 10 * 60

  private[this] val suffixSet = Set("m3u8","jpg", "gif", "png", "bmp", "ico", "swf",
    "css", "js", "htm", "html", "doc", "docx", "pdf", "ppt", "txt", "zip", "rar",
    "gz", "tgz", "exe", "xls", "cvs", "xlsx", "pptx", "mp3", "mp4", "rm", "mpeg",
    "wav", "m3u", "wma", "avi", "mov", "av", "flv", "f4v", "hlv", "f4fmp4", "f4fmp3",
    "ts", "dv", "fhv", "xml", "apk", "vxes", "ipa", "deb","jpeg")
    
  private[this] val uaList = List("android","iphone","ipad","letv","miui","firefox","chrome","linux","windows")
    
  def getResource(path: String): Array[Array[Long]] = {
    Source.fromInputStream(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(path)).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }
  val ispList = getResource("isp")
  val prvList = getResource("prv")

  
  /** kafka to  impala parse( 1:powza, 2:other ) **/
  def apply(line: String, channel: Channel): LogInfo = {
    try {
      val cdnlog_tag = line.substring(0, line.indexOf(" ", 0))
      if(cdnlog_tag.toUpperCase().contains("POWZA")) {
    	  parsePowzaLine(line, channel)
      } else {
    	  parseLine(line, channel)
      }
    } catch {
      case e: Throwable => {
        LogInfo.emptyLog()
      }
    }
  }

  def apply(line: String, channel: Channel, platform: PlatformInfo): BossLogInfo = {
    try {
      parseLinePlatform(line, channel, platform)
    } catch {
      case _: Throwable => { null }
    }
  }

  
  /** five minute band parse( 1:powza, 2:fastmedia, 3:other ) **/
  def apply(line: String, channel: Channel, logType: String): CsCountLog = {
    try {
      val cdnlog_tag = line.substring(0, line.indexOf(" ", 0)).toUpperCase()
      if(cdnlog_tag.contains("POWZA")) {
        parsePowzaLineCsLog(line, channel, "csCount")
      } else if (cdnlog_tag.contains("MEDIA")) {
    	  parseFastmediaLineCsLog(line, channel, "csCount")
      } else {
    	  parseLineCsLog(line, channel, "csCount")
      }
    } catch {
      case e: Throwable => { null }
    }
  }

  private def emptyLog(): LogInfo = {
    return null
  }

  def main(args: Array[String]) {
   val arr = Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("powza")).getLines().toArray
    for (log <- arr) {
      val logInfo = LogInfo(log, Channel.update(), "")
      val clazz = logInfo.getClass
      val arr = clazz.getDeclaredFields()
      for(i <- 0 to arr.length-1){
        val f = arr(i)
        f.setAccessible(true)
        print(f.getName + "：" + f.get(logInfo).toString() + "\t")
      }
      println()
    }
 }

  
  /** get domain, url, suffix, userid    param1: GET http://xxx.com/a/b/c.jpg  **/
  def getURLRelated(method_url_http: String, channel: Channel): Array[String] = {
    val url_o = method_url_http.split(" ")(1)
    val s_end = url_o.length
    val begin = url_o.indexOf("://") + 3
    var end = url_o.indexOf('/', begin)
    if (end < 0) end = s_end
    var domain = url_o.substring(begin, end)
    domain = channel.transform(domain)
    if (domain == null) throw new RuntimeException("domain is null")
    val userid = channel.getUserID(domain)
    end = url_o.indexOf('?', begin)
    if (end < 0) end = s_end
    val url = url_o.substring(begin, end)
    val dot = url.lastIndexOf('.')
    end = url.length - 1
    val suffix = if (dot < 0) {
      "other"
    } else {
      val suffix_o = url.substring(dot + 1)
      if (suffixSet.contains(suffix_o.toLowerCase)) {
        suffix_o.toLowerCase
      } else {
        "other"
      }
    }
    Array(domain, url, suffix, userid)
  }

  /**get url, suffix   param1:http://anhui.vtime.cntv.cloudcdn.net:80/cache/69/seg0/index.m3u8**/
  def getPowzaURLRelated(url_o: String, channel: Channel): Array[String] = {
    val s_end = url_o.length
    val begin = url_o.indexOf("://") + 3
    var end = url_o.indexOf('?', begin)
    if (end < 0) end = s_end
    val url = url_o.substring(begin, end)
    val dot = url.lastIndexOf('.')
    val suffix = if (dot < 0) {
      "other"
    } else {
      val suffix_o = url.substring(dot + 1)
      if (suffixSet.contains(suffix_o.toLowerCase)) {
        suffix_o.toLowerCase
      } else {
        "other"
      }
    }
    Array(url, suffix)
  }

  
  /**get domain   param1:http://anhui.vtime.cntv.cloudcdn.net:80/cache/69/seg0/index.m3u8**/
  def getDomain(url: String): String = {
    val begin = url.indexOf("://") + 3
    var end = url.indexOf('/', begin)
    if (end < 0) end = url.length
    url.substring(begin, end)
  }

  
  /**getUa: ua can change day one time  address:http://udap.fwlog.cachecn.net:8088/ua.conf**/
  def getUAType(uaString: String): String = {
//    UaConfUtil.update().getUaType(uaString)
    
    val ua_l = uaString.toLowerCase()
    uaList.foreach { x => 
      if(ua_l.contains(x)){
        return x
      }
    }
    return "other"
    
    /*
    if (ua_l.contains("inphic") || ua_l.contains("miui") || ua_l.contains("letv") || ua_l.contains("diyomate")) {
      "tv"
    } else if (ua_l.contains("android") || ua_l.contains("iphone") || ua_l.contains("ipad")) {
      "mobile"
    } else if (ua_l.contains("windows nt") || ua_l.contains("linux") || ua_l.contains("msie") || ua_l.contains("firefox") || ua_l.contains("chrome")) {
      "pc"
    } else {
      "other"
    }
    */
  }

  
  /**StringIP:192.168.168.168   to Long**/
  def ipToLong(ipString: String): Long = {
    val l = ipString.split("\\.").map((x: String) => x.toLong)
    var sum: Long = 0
    for (e <- l) {
      sum = (sum << 8) + e
    }
    sum
  }

  def timeToLong(time: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    format.parse(time).getTime / 1000
  }

  
  /**kafka to impala other data parse**/
  def parseLine(line: String, channel: Channel): LogInfo = {

    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }

    //get first 5 splits whose separator is space
    val first5 = new Array[String](5)
    var counter = 0
    while (counter < 5) {
      first5(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))
    val methodUrlHttp = next("\"")
    val domainUrlSuffix = getURLRelated(methodUrlHttp, channel)
    val status = next(' ')
    val cs = next(' ').toLong
    val refer_o = next('"')
    val refer = refer_o match {
      case "-" => "-"
      case _   => getDomain(refer_o)
    }
    val ua = getUAType(next('"'))
    val hit = if (!end) next(' ') else "unkown"
    var es = 0.0
    var dd = 0
    if (end) {
      es = if (first5(3).equals("-")) 0 else first5(3).toDouble
    } else {
      for (e <- 1 to 9) {
        next(' ')
      }

      es = first5(3) match {
        case "-" => 0
        case _   => first5(3).toDouble
      }
    }
    val ddStr = next(' ')
    dd = if (end) 0 else { try { ddStr.toInt } catch { case e: Throwable => { 0 } } }
    var rk = next(' ')
    var up = next(' ')
    var Fa = next(' ')
    var Fv = next(' ')
    
    var platform = "-"
    val platform_cdnlog = first5(0)
    if(platform_cdnlog.startsWith("@|@")){
      platform = try { platform_cdnlog.substring(platform_cdnlog.indexOf("@|@", 0) + 3, platform_cdnlog.indexOf("@|@", platform_cdnlog.indexOf("@|@", 0) + 3)) } catch { case e: Throwable => { "-" }}
    }
    
    val nodeStr = line.substring(line.lastIndexOf(" ", line.length()) + 1, line.length())
    
    val node = try { String.valueOf(ipToLong(nodeStr)) } catch { case e: Throwable => { "0" }}
    
    var ipStr = first5(2)
    
    if(ipStr.startsWith("@|@")){
      ipStr = ipStr.substring(ipStr.lastIndexOf("@|@", ipStr.length()) + 3, ipStr.length())
    }
    if (ipStr.startsWith("127")) {
      null
    } else {
      val ip = ipToLong(ipStr)
      LogInfo(first5(1), ip, search(ispList, ip).toString, search(prvList, ip).toString,
        domainUrlSuffix(0), domainUrlSuffix(1), domainUrlSuffix(2), status,
        cs, refer, ua, es, hit, time, dd, domainUrlSuffix(3), rk, up, Fa, Fv, platform, node)
    }
  }

  
  
  /**kafka to impala powza data parse**/
  def parsePowzaLine(line: String, channel: Channel): LogInfo = {
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }

    val first6 = new Array[String](6)
    var counter = 0
    while (counter < 6) {
      first6(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))
    val status = next(' ')
    val cs = next(' ').toLong
    val domain_http = next(' ')
    var domain = getDomain(domain_http)
    domain = channel.transform(domain)
    val userid = channel.getUserID(domain)
    val first2 = new Array[String](2)
    for(i <- 0 to 1){
      first2(i) = next(' ')
    }
    
    val urlSuffix = getPowzaURLRelated(next(' '), channel)
    
    val ua = getUAType(next('"'))
    val refer = "-"
    val hit = "unkown"
    val es = 0.0
    val dd = 0
    val rk = "-"
    val up = "-"
    val Fa = "-"
    val Fv = "-"
    val platform = "-"

    val nodeStr = line.substring(line.lastIndexOf(" ", line.length()) + 1, line.length())
    val node = try { String.valueOf(ipToLong(nodeStr)) } catch { case e: Throwable => { "0" }}
    
    var ipStr = first6(3)
    
    if(ipStr.startsWith("@|@")){
      ipStr = ipStr.substring(ipStr.lastIndexOf("@|@", ipStr.length()) + 3, ipStr.length())
    }
    if (ipStr.startsWith("127")) {
      null
    } else {
      val ip = ipToLong(ipStr)
      LogInfo(first6(1), ip, search(ispList, ip).toString, search(prvList, ip).toString,domain, 
          urlSuffix(0), urlSuffix(1), status,cs, refer, ua, es, hit, time, dd, userid, rk, up, Fa, Fv, platform, node)
    }
  }

  
  
  /**five minute band other data parse**/
  def parseLineCsLog(line: String, channel: Channel,logType:String): CsCountLog = {

    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }
    val first5 = new Array[String](5)
    var counter = 0
    while (counter < 5) {
      first5(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))
    val methodUrlHttp = next("\"")
    val domainUrlSuffix = getURLRelated(methodUrlHttp, channel)
    val status = next(' ')
    
    val cs = next(' ').toLong
    
    val platformName = "-"
    val popID = 0

    var ipStr = first5(2)

    if(ipStr.startsWith("@|@")){
      ipStr = ipStr.substring(ipStr.lastIndexOf("@|@", ipStr.length()) + 3, ipStr.length())
    }
    if (ipStr.startsWith("127")) {
      null
    } else {
      val ip = ipToLong(ipStr)
    	val prv = search(prvList, ip).toString
    	val prvCity = IPArea.update().getPrvAndCountry(prv)
      
      val es = try { first5(3).toDouble } catch { case e: Throwable => { 0.0 }}
      
      val nodeStr = line.substring(line.lastIndexOf(" ", line.length()) + 1, line.length())
    
      val node = try { ipToLong(nodeStr) } catch { case e: Throwable => { 0 }}
      
      CsCountLog(domainUrlSuffix(0), search(ispList, ip).toString, prv,prvCity._1,prvCity._2,cs,status,domainUrlSuffix(3),timeToLong(time),platformName,popID, node, es)
    }
  }
  
  
  
  /**five minute band powza data parse**/
  def parsePowzaLineCsLog(line: String, channel: Channel,logType:String): CsCountLog = {
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }
    
    val first6 = new Array[String](6)
    var counter = 0
    while (counter < 6) {
      first6(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))
    val status = next(' ')
    val cs = next(' ').toLong
    val domain_http = next(' ')
    var domain = getDomain(domain_http)
    domain = channel.transform(domain)
    val userid = channel.getUserID(domain)
    val first2 = new Array[String](2)
    for(i <- 0 to 1){
      first2(i) = next(' ')
    }
    val platform = "-"

    var ipStr = first6(3)
    
    if(ipStr.startsWith("@|@")){
      ipStr = ipStr.substring(ipStr.lastIndexOf("@|@", ipStr.length()) + 3, ipStr.length())
    }
    if (ipStr.startsWith("127")) {
      null
    } else {
      val ip = ipToLong(ipStr)
      val prv = search(prvList, ip).toString
      val prvCity = IPArea.update().getPrvAndCountry(prv)
      val popId = 0
      
      val es = try { first2(0).toDouble / 1000 } catch { case e: Throwable => 0.0 }
      
      val nodeStr = line.substring(line.lastIndexOf(" ", line.length()) + 1, line.length())
    
      val node = try { ipToLong(nodeStr) } catch { case e: Throwable => { 0 }}
      
      CsCountLog(domain, search(ispList, ip).toString, prv,prvCity._1, prvCity._2, cs, status, userid, timeToLong(time), platform, popId, node, es)
    }
  }

  
  
  /**five minute band fastmedia data parse**/
  def parseFastmediaLineCsLog(line: String, channel: Channel,logType:String): CsCountLog = {
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }
    val first4 = new Array[String](4)
    var counter = 0
    while (counter < 4) {
      first4(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))

    val first11 = new Array[String](11)
    
    for(i<- 0 to 10){
      first11(i) = next(' ')
    }
    
    var ipStr = first11(4)
    
    val domain = channel.transform(getDomain(first11(1)))
    
    val userid = channel.getUserID(domain)
    
    if(ipStr.startsWith("@|@")){
      ipStr = ipStr.substring(ipStr.lastIndexOf("@|@", ipStr.length()) + 3, ipStr.length())
    }
    if (ipStr.startsWith("127")) {
      null
    } else {
      val ip = ipToLong(ipStr)
      val prv = search(prvList, ip).toString
      val prvCity = IPArea.update().getPrvAndCountry(prv)
      val popId = 0
      
      val es = 0.0
      val platform = "-"
      val status = "0"
      
      val cs = first11(10).toLong
      
      val nodeStr = line.substring(line.lastIndexOf(" ", line.length()) + 1, line.length())
    
      val node = try { ipToLong(nodeStr) } catch { case e: Throwable => { 0 }}
      
      CsCountLog(domain, search(ispList, ip).toString, prv,prvCity._1, prvCity._2, cs, status, userid, timeToLong(time), platform, popId, node, es)
    }
  }

  
  
  def parseLinePlatform(line: String, channel: Channel, platform: PlatformInfo): BossLogInfo = {
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss Z"
    var index = -1
    var pre = 0
    var preSepState = false
    var end = false

    @tailrec
    def next(separator: Any): String = {
      if (end) return "-"
      val s = separator.toString
      s match {
        case " " => {
          if (preSepState) pre = index + 2 else pre = index + 1
          preSepState = false
        }
        case _ => {
          if (preSepState) pre = index + 3 else pre = index + 2
          preSepState = true
        }
      }
      index = line.indexOf(s, pre)
      index = if (index < 0) {
        end = true
        line.length
      } else index
      if (pre > index) {
        "-"
      } else {
        val r = line.substring(pre, index)
        if (r.equals("")) {
          next(' ')
        } else {
          r
        }
      }
    }

    //get first 5 splits whose separator is space
    val first5 = new Array[String](5)
    var counter = 0
    while (counter < 5) {
      first5(counter) = next(' ')
      counter = counter + 1
    }
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(next(']')))
    //TODO:fix url that contains '"', if not, use
    val methodUrlHttp = next("\"")
    val domainUrlSuffix = getURLRelated(methodUrlHttp, channel)
    val status = next(' ')
    val cs = next(' ').toLong
    val refer_o = next('"')
    val refer = refer_o match {
      case "-" => "-"
      case _   => getDomain(refer_o)
    }
    val ua = getUAType(next('"'))
    val hit = if (!end) next(' ') else "unkown"
    var es = 0.0
    var dd = 0
    if (end) {
      es = if (first5(3).equals("-")) 0 else first5(3).toDouble
    } else {
      for (e <- 1 to 9) {
        next(' ')
      }

      es = first5(3) match {
        case "-" => 0
        case _   => first5(3).toDouble
      }
    }
    val ddStr = next(' ')
    dd = if (end) 0 else { try { ddStr.toInt } catch { case e: Throwable => { 0 } } }
    var rk = next(' ')
    var up = next(' ')
    var Fa = next(' ')
    var Fv = next(' ')
    var ext1 = next(' ')
    var ext2 = next(' ')
    var ipStr = first5(2)
    if ("127.0.0.1".equals(ipStr)) {
      null
    } else {
      val ip = ipToLong(ipStr)

      //boss
      val serverid = first5(1)
      val platformName = platform.findName(serverid)
      val platformID = platform.findID(serverid)
      val platformIsp = platform.findIsp(serverid)
      val platformPrv = platform.findPrv(serverid)
      val platformIspCode = platform.findIspCode(serverid)
      val platformPrvCode = platform.findPrvCode(serverid)

      val req_num = 1
      val userid = domainUrlSuffix(3)
      val timestamp = timeToLong(time).toString
      val isp = search(ispList, ip).toString
      val prv = search(prvList, ip).toString
      val domain = domainUrlSuffix(0)
      val statusCode = status
      val speedSize = getSpeed(es, cs)
      var zeroSpeedReqNum = 0
      if (speedSize == 0) zeroSpeedReqNum = 1
      val speedInterval = "0"
      val flowSize = cs
      var fv = 0
      if (Fv.contains("HIT")) {
        fv += 1
      }
      val hitType = getHitType(hit, up, fv)

      BossLogInfo(serverid, timestamp, isp, prv, domain, statusCode, hitType,
        ua, speedInterval, speedSize, es, flowSize, req_num, userid, zeroSpeedReqNum,
        platformName, platformID, platformIsp, platformPrv, platformIspCode, platformPrvCode)

    }
  }

 
  
  /**get Hit Type  1:HIT  2:MISS**/
  def getHitType(hitString: String, up: String, fv: Int): Int = {
    val hit = hitString.toLowerCase()
    if (hit.contains("hit")) {
      HIT
    } else if (hit.contains("miss")) {
      if (up.length.equals(1) || is_in_father_iplist(up).equals(0)) {
        SOURCE
      } else if (up.length > 1 && is_in_father_iplist(up).equals(1)) {
        if (fv.equals(1)) {
          PARENT_HIT
        } else if (fv.equals(0)) {
          PARENT_MISS
        } else {
          MISS_OTHER
        }
      } else {
        MISS_OTHER
      }
    } else {
      MISS_OTHER
    }
  }

  def getSpeed(es: Double, cs: Long): Long = {
    var speed = 0L
    val tl_es = es.toLong
    if (tl_es != 0 && cs != 0) {
      speed = cs / 1024 / tl_es * 1000 // kb/s
    }

    if (speed < 5 || speed > 10240) { // || es/1000 < 2
      speed = 0L
    }
    return speed
  }

  def getSpeedInterval(cdnlogInfo: BossLogInfo): Array[Long] = {
    val result = Array(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
    val speeds = cdnlogInfo.speedInterval.split(",")
    for (x <- speeds) {
      try {
        val speed = x.split(":")
        result(speed(0).toInt) = speed(1).toLong
      } catch {
        case _: Throwable =>
      }
    }
    result
  }

  def is_in_father_iplist(up: String): Int = {
    ParentIP.update()
    if (ParentIP.current.IPlist.contains(up).toString.equals("true")) {
      1
    } else {
      0
    }
  }

  def isTimeout(LogInfo: BossLogInfo, current: Long) = {
    current > (LogInfo.timestamp.toLong + timeoutInterval)
  }

  def getHourTimestampBySecondTimestamp(secondTimestamp: String): String = {
    try {
      (secondTimestamp.toLong / 3600 * 3600).toString()
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  def getFiveTimestampBySecondTimestamp(secondTimestamp: Long): Long = {
    try {
      (secondTimestamp / 300 * 300 + 300)
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  
  /** one minute timestamp TO five minute timestamp **/
  def getTimestampByMinutesTimestamp(secondTimestamp: String): String = {
    try {
      (secondTimestamp.toLong / 60 * 60).toString()
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  
  /** get isp or prv from file(isp,prv) by user ip **/
  def search(list: Array[Array[Long]], ip: Long): Long = {
    def compare(ip: Long, line: Array[Long]): Int = {
      if (ip < line(0)) {
        -1
      } else if (ip > line(1)) {
        1
      } else {
        0
      }
    }

    @tailrec
    def searchIn(low: Int, high: Int): Long = {
      val m = (low + high) / 2
      val state = compare(ip, list(m))
      if (state == 0) {
        list(m)(2)
      } else if (high <= low) {
        0
      } else if (state < 0) {
        searchIn(low, m - 1)
      } else {
        searchIn(m + 1, high)
      }
    }
    searchIn(0, list.length - 1)
  }
}