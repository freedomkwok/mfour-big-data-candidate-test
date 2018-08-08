package com.utils

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object SparkHelper {
  private val log = Logger.getLogger(getClass.getName)
  private var sparkContext: SparkContext = null
  private var isLocal = true

  def context = {
    sparkContext
  }

  def init(appName: String, kryoClasses: List[Class[_]], tempDirectory: String, numOfCores: Int) {
    if (sparkContext != null ) {
      sparkContext.stop()
    }

    if(isLocal) {
      log.debug("Using local Spark as default.")
      sparkContext = createLocalContext(appName, kryoClasses, tempDirectory, numOfCores)
    }
    else {
        return createClusterContext(appName, tempDirectory: String, numOfCores: Int)
    }
  }

  def createLocalContext(appName: String, kryoClasses: List[Class[_]], tempDirectory: String, numOfCores: Int): SparkContext = {
    val sparkConfig = createSparkConfig(appName, kryoClasses)
    System.setProperty("hadoop.home.dir", tempDirectory)
    sparkConfig.setMaster(s"local[$numOfCores]")
    val sc = new SparkContext(sparkConfig)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", "")
    hadoopConf.set("fs.s3n.awsSecretAccessKey", "")
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", credentials.getAWSAccessKeyId)
    hadoopConf.set("fs.s3.awsSecretAccessKey", credentials.getAWSSecretKey)
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.fast.upload", "true")
    hadoopConf.set("fs.s3a.access.key", credentials.getAWSAccessKeyId)
    hadoopConf.set("fs.s3a.secret.key", credentials.getAWSSecretKey)
    isLocal = true
    return sc
  }

  def createClusterContext(appName: String, kryoClasses: List[Class[_]]): SparkContext = {
    val sparkConfig = createSparkConfig(appName, kryoClasses)
    isLocal = false
    return new SparkContext(sparkConfig)
  }

  def createSparkConfig(appName: String, kryoClasses: List[Class[_]]): Unit = {
    val sparkConfig = new SparkConf().setAppName(appName)
    sparkConfig.set("spark.hadoop.validateOutputSpecs", "false")
    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConfig.set("spark.kryo.referenceTracking", "false")
    sparkConfig.set("spark.rpc.message.maxSize", "500")

    if ((kryoClasses != null) && (!kryoClasses.isEmpty)) {
      sparkConfig.registerKryoClasses(kryoClasses.toArray)
    }
    return sparkConfig
  }
}