package com.excercise

import org.apache.log4j.Logger

object SparkExercise extends Serializable {
  def main(arg: Array[String]) {
      var log = Logger.getLogger(getClass.getName)
      try {
        log.info("asd")
        }
       catch {
        case ex: Exception =>
          log.error("General error", ex)
          throw ex
      }
  }
}