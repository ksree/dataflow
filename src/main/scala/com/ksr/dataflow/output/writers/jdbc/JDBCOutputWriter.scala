package com.ksr.dataflow.output.writers.jdbc

import java.util.Properties

import com.ksr.dataflow.configuration.job.output.JDBC
import com.ksr.dataflow.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

class JDBCOutputWriter(props: Map[String, String], jdbcConf: Option[JDBC]) extends Writer {

  case class JDBCOutputProperties(saveMode: SaveMode, dbTable: String)

  @transient lazy val log = LogManager.getLogger(this.getClass)
  val dbOptions = JDBCOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"))

  override def write(dataFrame: DataFrame): Unit = {
    log.info(s"Writing into JDBC")
    jdbcConf match {
      case Some(jdbcConf) =>
        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcConf.user)
        connectionProperties.put("password", jdbcConf.password)
        connectionProperties.put("driver", jdbcConf.driver)
        var df = dataFrame
        val writer = df.write
          .format(jdbcConf.driver)
          .mode(dbOptions.saveMode)
          .jdbc(jdbcConf.connectionUrl, dbOptions.dbTable, connectionProperties)
      case None =>
    }
  }
}
