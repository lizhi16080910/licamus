package com.fastweb.cdnlog.bigdata.analysis

/**
  * Created by lfq on 2017/1/4.
  */
object Bins {
    val max=10000.0
    val min=0.0

    def convert(value : Double): Long ={
         round(value)
    }

    def round(value : Double):Long ={
        Math.round(value)
    }

    def main(args: Array[String]) {
         println(convert(99.5))
    }
}
