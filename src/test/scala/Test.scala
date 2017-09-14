import java.util

import scala.collection.mutable
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
  * Created by lfq on 2017/3/17.
  */
object Test {
    def main(args: Array[String]) {
        println(matchTest("two"))
        println(matchTest("test"))
        println(matchTest(1))
        println(matchTest(2))

    }
    def matchTest(x: Any): Any = x match {
        case 1 => "one"
        case "two" => 2
        case y: Int => "scala.Int"
        case _ => "many"
    }
}
