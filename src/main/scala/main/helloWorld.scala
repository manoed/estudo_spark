package main

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import scala.collection.Seq

/**
 *
 * Classe de estudos e teste.
 *
 *
 * @author Edgard
 * @version 1.0
 * @since   03-08-2018
 *
 */

object helloWorld {
  def main(args: Array[String]) {
    val spark = SparkSession.
      builder().
      appName("uciEvasaoInvestDiariaPF").
      enableHiveSupport().
      getOrCreate();

    val sqlSpark = spark.sqlContext;
    val log = LogManager.getRootLogger;
    log.setLevel(Level.WARN);

    import org.apache.spark.sql.functions._;
    import sqlSpark.implicits._;
    import sqlSpark.implicits.StringToColumn;
    import java.time.LocalDate;
    import java.time.format.DateTimeFormatter;

    sqlSpark.sql("set spark.sql.hive.convertMetastoreOrc=true");
    sqlSpark.setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sqlSpark.setConf("spark.shuffle.blockTransferService", "nio");

    /* Obtem variaveis de data */
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    val dt_atual = LocalDate.now().format(dtf);

    println("Iniciando o processamento...");

    sqlSpark.sql("drop table if exists risk_factor_spark")
    val geolocation_temp1 = sqlSpark.sql("select * from geolocation");
    val drivermileage_temp1 = sqlSpark.sql("select * from drivermileage")

    val geolocation_temp2 = geolocation_temp1.filter(col("event") =!= "normal").groupBy("driverid").agg(count("driverid").as("occurance"))
    val joined = geolocation_temp2.join(drivermileage_temp1, Seq("driverid"), "inner")
    val risk_factor_spark = joined.withColumn("riskfactor", col("totmiles") / col("occurance")).
      withColumn("dt_ref", to_date(lit(dt_atual)))
    val risk_factor_spark_fim = risk_factor_spark.repartition(1);
    risk_factor_spark.createOrReplaceTempView("risk_factor_spark")
    sqlSpark.sql("create table risk_factor_spark stored as orc as select * from risk_factor_spark")
  }
}