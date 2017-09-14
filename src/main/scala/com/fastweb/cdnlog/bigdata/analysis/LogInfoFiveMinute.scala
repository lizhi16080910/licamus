package com.fastweb.cdnlog.bigdata.analysis

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.io.Source
import scala.util.matching._

case class LogInfoFiveMinute(domain: String, userid: String,cs:Long,fiveMinuteTime: Long,isp: String,prv: String,area_pid:String,area_cid:String)

/**
 * parse nginx logs.
 *
 * Usage: LogInfoFiveMinute(log,channel.update)
 */

object LogInfoFiveMinute {
  private[this] val log = Logger.getLogger(this.getClass)
  def getResource(path: String): Array[Array[Long]] = {
    Source.fromInputStream(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(path)).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }
  val ispList = getResource("isp")
  val prvList = getResource("prv")


  def apply(line: String, channel: Channel): LogInfoFiveMinute = {
    try {
      parseLine(line, channel)
    } catch {
      case e: Exception => {
        log.error(line, e)
        null
      }
    }
  }



  def parseLine(line: String, channel: Channel): LogInfoFiveMinute = {
    val timeFormat = "dd/MMM/yyyy:HH:mm:ss"
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var fiveMinuteTime= 0L

    //获取用户IP,过滤用户Ip为127.0.0.1
    val  ipRegex = new Regex("""((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))""")
    val ip = ipRegex.findFirstMatchIn(line).getOrElse(null)
    log.debug("regex catch the ip="+ip)
    if (ip.toString().equals("127.0.0.1")){
      //log.error(line)
      return  null
    }

    //获取isp,prv,areacid
    val ipStr = ipToLong(ip.toString())
    val isp =  search(ispList, ipStr).toString;
    val prv = search(prvList, ipStr).toString
    val prvCity =  IPArea.update().getPrvAndCountry(prv)

    //获取时间
    val dateTimeRegex = new Regex("""\[([\w\d:/]+\s[+\-]\d{3,4})\]""")
    val logTime = dateTimeRegex.findFirstMatchIn(line).getOrElse(null)
    log.debug("regex catch the logTime ="+logTime)
    if(logTime != null) {
      val time = dateFormat.format(new SimpleDateFormat(timeFormat, Locale.ENGLISH).parse(logTime.toString().replace("[","").replace("]","")))
      fiveMinuteTime =  getFiveTimestampBySecondTimestamp(timeToLong(time))
    }else{
      log.error("logTime is null:"+line)
      return  null
    }

    //获取domain,userid,cs，金山日志状态码后面为 - ，往后顺延一个空格，取流量
    val urlRegex =new Regex("""([a-zA-z]+://[^\s]*|[a-zA-z]+://[^\s]*\s\S*) ([0-9]{0,3}) (\d*|\-) (\d*)""")
    log.debug("regex catch the  url = "+urlRegex.findFirstMatchIn(line).mkString)

    val domain =  {
      urlRegex.findFirstMatchIn(line) match {
        case Some(urlRegex(url, _, bytes,ksswsBytes)) =>
          val domain = getDomain(url.toString, channel)
          if(domain == null){
            log.error("domain is null:"+line)
            return null
          }
          //判断流量,金山日志特殊处理
          log.debug("parse log bytes="+bytes+","+"ksswsBytes="+ksswsBytes)
          var cs = ""
          if(bytes.toString.equals("-")){
            cs = ksswsBytes
          }else{
            cs = bytes
          }
          if(cs == ""){
            log.error("cs is null:"+line)
            return null
          }

          Array[String](domain(0), domain(1), cs)

        case _ =>  null
      }
    }

    log.debug("domian="+domain)

    if (domain == null ){
      log.error("domain list is null:" + line)
      return null
    }else{
      LogInfoFiveMinute(domain(0),domain(1),domain(2).toLong,fiveMinuteTime,isp,prv,prvCity._1,prvCity._2)
    }

  }

  def ipToLong(ipString: String): Long = {
    val l = ipString.split("\\.").map((x: String) => x.toLong)
    var sum: Long = 0
    for (e <- l) {
      sum = (sum << 8) + e
    }
    sum
  }

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


  def getDomain(method_url_http: String, channel: Channel): Array[String] = {
    val url_o = method_url_http
    val s_end = url_o.length
    val begin = url_o.indexOf("://") + 3
    var end = url_o.indexOf('/', begin)
    if (end < 0) end = s_end
    var domain = url_o.substring(begin, end)
    domain = clearDomain(domain);

    var userid = ""
    log.debug("domian in source log="+domain)
    domain = channel.transform(domain)
    if(domain != null) userid = channel.getUserID(domain)
    log.debug("domian transform after="+domain)
    if (domain == null || userid == "") return null

    Array(domain, userid)
  }


  //去掉域名结尾的特殊字符,如：s6.pstatp.com.
  def clearDomain(domain:String):String = {
    var lastchar = domain.charAt(domain.length-1)
    if (!lastchar.isLetterOrDigit){
      var tranDomain = domain.substring(0,domain.length-1)
      clearDomain(tranDomain);
    }else{
      return domain
    }


  }

  def timeToLong(time: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    format.parse(time).getTime / 1000
  }

  def getFiveTimestampBySecondTimestamp(secondTimestamp: Long): Long = {
    try {
      (secondTimestamp / 300 * 300 + 300)
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  def main(args: Array[String]) {

    val channel = Channel.update()

    val log1 = """CDNLOG ctl-hb-119-097-146-040 218.76.89.74 0.000 - [07/Jun/2016:16:48:44 +0800] "GET http://img.baibwang.com/template/img/l2/x.png?_1453688577786 HTTP/1.1" 200 683 "http://www.baibwang.com/l2/task.html?t=1465289307815" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C; .NET4.0E)" FCACHE_HIT_DISK  0.000 0.000 - - - - 0.000 0.000 0.000 1 - - 218.76.89.74 "DISK HIT from 183.136.205.74, DISK HIT from 183.136.205.77, DISK HIT from 119.97.146.40" 255 119.97.146.40"""


    val log3="""CDNLOG bgp-tj-113-031-028-076 26848_283212_1466435441117 s.vdo17v.com 119.90.62.55 [20/Jun/2016:23:11:05 +0800] PLAY "rtmp://s.vdo17v.com/agin/4-21" ""430 332527 0 streaming:status "" "LNX.11,1,102,55" (24s) HIT From s.vdo17v.com, -  113.31.28.76"""


    var log4 = """CDNLOG cmb-js-183-213-022-035 218.205.20.92 0.000 - [08/Jun/2016:12:37:45 +0800] "GET http://static.bshare.cn/b/addons/bshareDrag.js?bp=qqmb,sinaminiblog,qzone,kaixin001,bsharesync&text=鍒嗕韩鍒?HTTP/1.1" 304 211 "http://www.vodjk.com/jjcs/151023/126085.shtml" "Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko; MSIE 7.0" FCACHE_MISS  0.032 0.000 - - - - 0.000 0.000 0.000 "LGenerate MISS from 183.213.22.35" 183.213.22.35"""

    var log5 = """CDNLOG cnc-sd-027-209-182-016 - - [29/Jun/2016:15:47:20 +800] 27.208.65.117 - 08076bff106b4e1cbb8a9afd4ca4a4e7 - - "GET http://ks.97ting.com/content/01/466/1466846-MP3-128K-FTD.mp3?token=039be58923f8215c94815a33ba1b866f&ts=1467359093921&hash=850da7f6b1f77ada6ebb318f1288041e&transDeliveryCode=XM@35@1467186293 HTTP/1.1" 200 - 690118 - 0 null "-" "MiuiMusic/4.4.4 (Linux; U; Android 4.4.4; 2014811 Build/KTU84P)" KS_FWLOG_TAG 27.209.182.16"""

    var log6=""" CDNLOG ctl-gd-113-107-250-013 NzM0MDEzMTc= media [29/Jun/2016:15:52:34 +800] 14.218.52.29 Anonymous d298ab5128f747de889afb27d620d123 REST.GET.OBJECT /content/01/496/1496752-MP3-320K-FTD.mp3 "GET http://ks.97ting.com/content/01/496/1496752-MP3-320K-FTD.mp3?token=04f59bea874d131498f69d198522365e&ts=1467359549816&hash=b5ca6fbb4941a1c5ea16ea576b31cdfe&transDeliveryCode=XM@35@1467186749 HTTP/1.1" 200 - 7873929 7873428 0 null "-" "MiuiMusic/5.0.2 (Linux; U; Android 5.0.2; Redmi Note 3 Build/LRX22G)" KS_FWLOG_TAG 113.107.250.13"""


    var log10 = """CDNLOG ctl-yn-112-117-208-021  0.115 - [29/Jun/2016:16:36:24 +0800] "HEAD http://dd.myapp.com/16891/6087D63950CDEF7ABE623094D88D9C89.apk HTTP/1.0" 200 374 "http://dd.myapp.com/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; .NET CLR 2.0.50727; .NET4.0C; .NET4.0E)" "116.53.87.190" 112.117.208.21"""

    var log11="""ctl-gd-183-056-172-030 171.107.86.8 0.082 - [29/Jul/2016:15:53:16 +0800] "GET http://s6.pstatp.com./monitor.jpg?xcode=d11444a92ddb87b5b5255c09b7721e314c932f26aaa77f9c&=1a81b0bbd448fc368d78cc336e28561a&t=1385635446111=1 HTTP/1.1" 403 302 "-" "ChinaCache" FCACHE_MISS  0.000 0.000 - - - - 0.000 0.082 0.082 0 - - 171.107.86.8 "LACL MISS from CTL_GD_172_030.fcd" 156 183.56.172.30"""


    var log12 = """CDNLOG ctl-hb-219-138-025-028 219.138.135.204 0.000 - [29/Jul/2016:17:04:32 +0800] "GET http://bbs.voc.com.cn/b/addons/bshareDrag.js?bp=bsharesync,qzone,qqmb,sinaminiblog,renren&logo=false HTTP/1.1" 403 277 "http://bbs.voc.com.cn/topic-7373686-1-1.html" "Mozilla/5.0 (Windows NT 6.1; rv:24.0) Gecko/20100101 Firefox/24.0" FCACHE_MISS  0.044 0.000 - - - - 0.000 0.000 0.000 "-" 219.138.25.28"""


    var log13=""" CDNLOG cnc-sd-123-133-084-040 123.133.84.43 0.079 - [29/Jul/2016:17:13:11 +0800] "GET http://123.133.84.40/lvs_keepalived_check HTTP/1.0" 403 310 "-" "KeepAliveClient" FCACHE_MISS 0 - - 123.133.84.43 "LACL MISS from cnc-sd-123-133-084-040.fcd" 156 123.133.84.40"""

    println( LogInfoFiveMinute(log11,channel))


    /*val arr = Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("fiveMinuteTest.log")).getLines().toArray
    for (content <- arr) {
      val dom = LogInfoFiveMinute(content,channel)
      if (dom !=null) log.info("域名:" + dom.domain + "\t用户ID:" + dom.userid + "\t时间:" + dom.fiveMinuteTime + "\t流量:" + dom.cs )
    }*/

  }





}
