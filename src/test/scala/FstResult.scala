import java.util
import com.fastweb.cdnlog.bigdata.util.{FileUtil, IPUtil}

import scala.io.Source


/**
  * Created by lfq on 2017/1/6.
  */
class FstResult(val url:String,val ip:String,val count: Long) extends Comparable[FstResult]{
    override def compareTo(o: FstResult): Int = {
        if(o.count > this.count) 1 else { if(o.count < this.count) -1 else 0}
    }

}

object FstResult {
    def main(args: Array[String]) {
        val localfile = Source.fromFile("C:\\Users\\lfq\\Desktop\\fsf.result") //中文乱码
        val hour="11"
        val type1 = "size"
        val path = "C:\\Users\\lfq\\Desktop\\fst\\11点size.csv"
        val result = new util.ArrayList[String]()
        for(line <- localfile.getLines()){
            val temp = line.split(",")
            if(temp(3).equals(hour) && temp(4).equals(type1)){
                val url = temp(0)
                val ip = IPUtil.int2ip(temp(1).toLong)
                val count = temp(2)
                result.add(s"${url},${ip},${count}")
            }
        }
        FileUtil.listToFile(path,result)
        localfile.close()
        val a1 = new FstResult("fsf.meixiangdao.com/member/my.html","2090932029",2)
        val a2 = new FstResult("fsf.meixiangdao.com/member/my.html","2090932029",2)
        println(a2.compareTo(a1))
    }
}
