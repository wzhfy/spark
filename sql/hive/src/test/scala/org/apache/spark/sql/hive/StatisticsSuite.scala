/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import java.io.{File, PrintWriter}
import java.sql.{Date, Timestamp}

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{BasicColStats, Statistics}
import org.apache.spark.sql.execution.command.{AnalyzeTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{StructType, _}

class StatisticsSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  test("parse analyze commands") {
    def assertAnalyzeCommand(analyzeCommand: String, c: Class[_]) {
      val parsed = spark.sessionState.sqlParser.parsePlan(analyzeCommand)
      val operators = parsed.collect {
        case a: AnalyzeTableCommand => a
        case o => o
      }

      assert(operators.size === 1)
      if (operators(0).getClass() != c) {
        fail(
          s"""$analyzeCommand expected command: $c, but got ${operators(0)}
             |parsed command:
             |$parsed
           """.stripMargin)
      }
    }

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS nOscAn",
      classOf[AnalyzeTableCommand])
  }

  test("MetastoreRelations fallback to HDFS for size estimation") {
    val enableFallBackToHdfsForStats = spark.sessionState.conf.fallBackToHdfsForStatsEnabled
    try {
      withTempDir { tempDir =>

        // EXTERNAL OpenCSVSerde table pointing to LOCATION

        val file1 = new File(tempDir + "/data1")
        val writer1 = new PrintWriter(file1)
        writer1.write("1,2")
        writer1.close()

        val file2 = new File(tempDir + "/data2")
        val writer2 = new PrintWriter(file2)
        writer2.write("1,2")
        writer2.close()

        sql(
          s"""CREATE EXTERNAL TABLE csv_table(page_id INT, impressions INT)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
              \"separatorChar\" = \",\",
              \"quoteChar\"     = \"\\\"\",
              \"escapeChar\"    = \"\\\\\")
            LOCATION '$tempDir'
          """)

        spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, true)

        val relation = spark.sessionState.catalog.lookupRelation(TableIdentifier("csv_table"))
          .asInstanceOf[MetastoreRelation]

        val properties = relation.hiveQlTable.getParameters
        assert(properties.get("totalSize").toLong <= 0, "external table totalSize must be <= 0")
        assert(properties.get("rawDataSize").toLong <= 0, "external table rawDataSize must be <= 0")

        val sizeInBytes = relation.statistics.sizeInBytes
        assert(sizeInBytes === BigInt(file1.length() + file2.length()))
      }
    } finally {
      spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, enableFallBackToHdfsForStats)
      sql("DROP TABLE csv_table ")
    }
  }

  test("analyze MetastoreRelations") {
    def queryTotalSize(tableName: String): BigInt =
      spark.sessionState.catalog.lookupRelation(TableIdentifier(tableName)).statistics.sizeInBytes

    // Non-partitioned table
    sql("CREATE TABLE analyzeTable (key STRING, value STRING)").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()

    sql("ANALYZE TABLE analyzeTable COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable") === BigInt(11624))

    sql("DROP TABLE analyzeTable").collect()

    // Partitioned table
    sql(
      """
        |CREATE TABLE analyzeTable_part (key STRING, value STRING) PARTITIONED BY (ds STRING)
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-01')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-02')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-03')
        |SELECT * FROM src
      """.stripMargin).collect()

    assert(queryTotalSize("analyzeTable_part") === spark.sessionState.conf.defaultSizeInBytes)

    sql("ANALYZE TABLE analyzeTable_part COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable_part") === BigInt(17436))

    sql("DROP TABLE analyzeTable_part").collect()

    // Try to analyze a temp table
    sql("""SELECT * FROM src""").createOrReplaceTempView("tempTable")
    intercept[AnalysisException] {
      sql("ANALYZE TABLE tempTable COMPUTE STATISTICS")
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tempTable"), ignoreIfNotExists = true, purge = false)
  }

  private def checkTableStats(
      stats: Option[Statistics],
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Unit = {
    if (hasSizeInBytes || expectedRowCounts.nonEmpty) {
      assert(stats.isDefined)
      assert(stats.get.sizeInBytes > 0)
      assert(stats.get.rowCount === expectedRowCounts)
    } else {
      assert(stats.isEmpty)
    }
  }

  private def checkTableStats(
      tableName: String,
      isDataSourceTable: Boolean,
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Option[Statistics] = {
    val df = sql(s"SELECT * FROM $tableName")
    val stats = df.queryExecution.analyzed.collect {
      case rel: MetastoreRelation =>
        checkTableStats(rel.catalogTable.stats, hasSizeInBytes, expectedRowCounts)
        assert(!isDataSourceTable, "Expected a Hive serde table, but got a data source table")
        rel.catalogTable.stats
      case rel: LogicalRelation =>
        checkTableStats(rel.catalogTable.get.stats, hasSizeInBytes, expectedRowCounts)
        assert(isDataSourceTable, "Expected a data source table, but got a Hive serde table")
        rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }

  test("test table-level statistics for hive tables created in HiveExternalCatalog") {
    val textTable = "textTable"
    withTable(textTable) {
      // Currently Spark's statistics are self-contained, we don't have statistics until we use
      // the `ANALYZE TABLE` command.
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      checkTableStats(
        textTable,
        isDataSourceTable = false,
        hasSizeInBytes = false,
        expectedRowCounts = None)
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      checkTableStats(
        textTable,
        isDataSourceTable = false,
        hasSizeInBytes = false,
        expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats2 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("test elimination of the influences of the old stats") {
    val textTable = "textTable"
    withTable(textTable) {
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats1 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))

      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // when the total size is not changed, the old row count is kept
      val fetchedStats2 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1 == fetchedStats2)

      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // update total size and remove the old and invalid row count
      val fetchedStats3 = checkTableStats(
        textTable, isDataSourceTable = false, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats3.get.sizeInBytes > fetchedStats2.get.sizeInBytes)
    }
  }

  test("test statistics of LogicalRelation converted from MetastoreRelation") {
    val parquetTable = "parquetTable"
    val orcTable = "orcTable"
    withTable(parquetTable, orcTable) {
      sql(s"CREATE TABLE $parquetTable (key STRING, value STRING) STORED AS PARQUET")
      sql(s"CREATE TABLE $orcTable (key STRING, value STRING) STORED AS ORC")
      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      sql(s"INSERT INTO TABLE $orcTable SELECT * FROM src")

      // the default value for `spark.sql.hive.convertMetastoreParquet` is true, here we just set it
      // for robustness
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkTableStats(
          parquetTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
        checkTableStats(
          parquetTable,
          isDataSourceTable = true,
          hasSizeInBytes = true,
          expectedRowCounts = Some(500))
      }
      withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "true") {
        checkTableStats(
          orcTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $orcTable COMPUTE STATISTICS")
        checkTableStats(
          orcTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = Some(500))
      }
    }
  }

  test("test table-level statistics for data source table created in HiveExternalCatalog") {
    val parquetTable = "parquetTable"
    withTable(parquetTable) {
      sql(s"CREATE TABLE $parquetTable (key STRING, value STRING) USING PARQUET")
      val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(parquetTable))
      assert(DDLUtils.isDatasourceTable(catalogTable))

      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = false, expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = None)

      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
      val fetchedStats2 = checkTableStats(
        parquetTable, isDataSourceTable = true, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
      val fetchedStats3 = checkTableStats(
        parquetTable,
        isDataSourceTable = true,
        hasSizeInBytes = true,
        expectedRowCounts = Some(1000))
      assert(fetchedStats3.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("statistics collection of a table with zero column") {
    val table_no_cols = "table_no_cols"
    withTable(table_no_cols) {
      val rddNoCols = sparkContext.parallelize(1 to 10).map(_ => Row.empty)
      val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
      dfNoCols.write.format("json").saveAsTable(table_no_cols)
      sql(s"ANALYZE TABLE $table_no_cols COMPUTE STATISTICS")
      checkTableStats(
        table_no_cols,
        isDataSourceTable = true,
        hasSizeInBytes = true,
        expectedRowCounts = Some(10))
    }
  }

  private def checkColStats(
      rowRDD: RDD[Row],
      schema: StructType,
      expectedColStatsSeq: Seq[(String, BasicColStats)]): Unit = {
    val table = "tbl"
    withTable(table) {
      var df = spark.createDataFrame(rowRDD, schema)
      df.write.format("json").saveAsTable(table)
      val columns = expectedColStatsSeq.map(_._1).mkString(", ")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS $columns")
      df = sql(s"SELECT * FROM $table")
      val stats = df.queryExecution.analyzed.collect {
        case rel: LogicalRelation =>
          expectedColStatsSeq.foreach { expected =>
            assert(rel.catalogTable.get.stats.get.basicColStats.contains(expected._1))
            checkColStats(colStats = rel.catalogTable.get.stats.get.basicColStats(expected._1),
              expectedColStats = expected._2)
          }
      }
      assert(stats.size == 1)
    }
  }

  private def checkColStats(colStats: BasicColStats, expectedColStats: BasicColStats): Unit = {
    assert(colStats.dataType == expectedColStats.dataType)
    assert(colStats.numNulls == expectedColStats.numNulls)
    colStats.dataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        assert(colStats.max.map(_.toString.toLong) == expectedColStats.max.map(_.toString.toLong))
        assert(colStats.min.map(_.toString.toLong) == expectedColStats.min.map(_.toString.toLong))
      case FloatType | DoubleType =>
        assert(colStats.max.map(_.toString.toDouble) == expectedColStats.max
          .map(_.toString.toDouble))
        assert(colStats.min.map(_.toString.toDouble) == expectedColStats.min
          .map(_.toString.toDouble))
      case DecimalType.SYSTEM_DEFAULT =>
        assert(colStats.max.map(i => Decimal(i.toString)) == expectedColStats.max
          .map(i => Decimal(i.toString)))
        assert(colStats.min.map(i => Decimal(i.toString)) == expectedColStats.min
          .map(i => Decimal(i.toString)))
      case DateType | TimestampType =>
        if (expectedColStats.max.isDefined) {
          // just check the difference to exclude the influence of timezones
          assert(colStats.max.get.toString.toLong - colStats.min.get.toString.toLong ==
            expectedColStats.max.get.toString.toLong - expectedColStats.min.get.toString.toLong)
        } else {
          assert(colStats.max.isEmpty && colStats.min.isEmpty)
        }
      case _ => // only numeric types, date type and timestamp type have max and min stats
    }
    colStats.dataType match {
      case BinaryType => assert(colStats.ndv.isEmpty)
      case BooleanType => assert(colStats.ndv.contains(2))
      case _ =>
        // ndv is an approximate value, so we just make sure we have the value
        assert(colStats.ndv.get >= 0)
    }
    assert(colStats.avgColLen == expectedColStats.avgColLen)
    assert(colStats.maxColLen == expectedColStats.maxColLen)
    assert(colStats.numTrues == expectedColStats.numTrues)
    assert(colStats.numFalses == expectedColStats.numFalses)
  }

  test("basic statistics for integral type columns") {
    val rdd = sparkContext.parallelize(Seq("1", null, "2", "3", null)).map { i =>
      if (i != null) Row(i.toByte, i.toShort, i.toInt, i.toLong) else Row(i, i, i, i)
    }
    val schema = StructType(
      StructField(name = "c1", dataType = ByteType, nullable = true) ::
      StructField(name = "c2", dataType = ShortType, nullable = true) ::
      StructField(name = "c3", dataType = IntegerType, nullable = true) ::
      StructField(name = "c4", dataType = LongType, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(
      dataType = ByteType, numNulls = 2, max = Some(3), min = Some(1), ndv = Some(3))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = ShortType)),
      ("c3", expectedBasicStats.copy(dataType = IntegerType)),
      ("c4", expectedBasicStats.copy(dataType = LongType)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for fractional type columns") {
    val rdd = sparkContext.parallelize(Seq(null, "1.01", "2.02", "3.03")).map { i =>
      if (i != null) Row(i.toFloat, i.toDouble, Decimal(i)) else Row(i, i, i)
    }
    val schema = StructType(
      StructField(name = "c1", dataType = FloatType, nullable = true) ::
      StructField(name = "c2", dataType = DoubleType, nullable = true) ::
      StructField(name = "c3", dataType = DecimalType.SYSTEM_DEFAULT, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(
      dataType = FloatType, numNulls = 1, max = Some(3.03), min = Some(1.01), ndv = Some(3))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = DoubleType)),
      ("c3", expectedBasicStats.copy(dataType = DecimalType.SYSTEM_DEFAULT)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for string column") {
    val rdd = sparkContext.parallelize(Seq(null, "a", "bbbb", "cccc")).map(Row(_))
    val schema = StructType(StructField(name = "c1", dataType = StringType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = StringType, numNulls = 1,
      maxColLen = Some(4), avgColLen = Some(2.25), ndv = Some(3))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for binary column") {
    val rdd = sparkContext.parallelize(Seq(null, "a", "bbbb", "cccc")).map { i =>
      if (i != null) Row(i.getBytes) else Row(i)
    }
    val schema = StructType(StructField(name = "c1", dataType = BinaryType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = BinaryType, numNulls = 1,
      maxColLen = Some(4), avgColLen = Some(2.25))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for boolean column") {
    val rdd = sparkContext.parallelize(Seq(null, true, false, true)).map(Row(_))
    val schema =
      StructType(StructField(name = "c1", dataType = BooleanType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = BooleanType, numNulls = 1,
      numTrues = Some(2), numFalses = Some(1))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for date column") {
    val rdd = sparkContext.parallelize(Seq(null, "1970-01-01", "1970-02-02")).map { i =>
      if (i != null) Row(Date.valueOf(i)) else Row(i)
    }
    val schema =
      StructType(StructField(name = "c1", dataType = DateType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = DateType, numNulls = 1,
      max = Some(32), min = Some(0), ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for timestamp column") {
    val rdd = sparkContext.parallelize(Seq(null, "1970-01-01 00:00:00", "1970-01-01 00:00:05"))
      .map(i => if (i != null) Row(Timestamp.valueOf(i)) else Row(i))
    val schema =
      StructType(StructField(name = "c1", dataType = TimestampType, nullable = true) :: Nil)
    val statsSeq = Seq(("c1", BasicColStats(dataType = TimestampType, numNulls = 1,
      max = Some(5000000), min = Some(0), ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for null columns") {
    val rdd = sparkContext.parallelize(Seq(Row(null, null)))
    val schema = StructType(
      StructField(name = "c1", dataType = LongType, nullable = true) ::
      StructField(name = "c2", dataType = TimestampType, nullable = true) :: Nil)
    val expectedBasicStats = BasicColStats(dataType = LongType, numNulls = 1,
      max = None, min = None, ndv = Some(0))
    val statsSeq = Seq(
      ("c1", expectedBasicStats),
      ("c2", expectedBasicStats.copy(dataType = TimestampType)))
    checkColStats(rdd, schema, statsSeq)
  }

  test("basic statistics for columns with different types") {
    val rdd = sparkContext.parallelize(Seq(
      Row(1, 1.01, "a", "a".getBytes, true, Date.valueOf("1970-01-01"),
        Timestamp.valueOf("1970-01-01 00:00:00"), 5.toLong),
      Row(2, 2.02, "bb", "bb".getBytes, false, Date.valueOf("1970-02-02"),
        Timestamp.valueOf("1970-01-01 00:00:05"), 4.toLong)))
    val schema = StructType(Seq(
      StructField(name = "c1", dataType = IntegerType, nullable = false),
      StructField(name = "c2", dataType = DoubleType, nullable = false),
      StructField(name = "c3", dataType = StringType, nullable = false),
      StructField(name = "c4", dataType = BinaryType, nullable = false),
      StructField(name = "c5", dataType = BooleanType, nullable = false),
      StructField(name = "c6", dataType = DateType, nullable = false),
      StructField(name = "c7", dataType = TimestampType, nullable = false),
      StructField(name = "c8", dataType = LongType, nullable = false)))
    val statsSeq = Seq(
      ("c1", BasicColStats(dataType = IntegerType, numNulls = 0, max = Some(2), min = Some(1),
        ndv = Some(2))),
      ("c2", BasicColStats(dataType = DoubleType, numNulls = 0, max = Some(2.02), min = Some(1.01),
        ndv = Some(2))),
      ("c3", BasicColStats(dataType = StringType, numNulls = 0, maxColLen = Some(2),
        avgColLen = Some(1.5), ndv = Some(2))),
      ("c4", BasicColStats(dataType = BinaryType, numNulls = 0, maxColLen = Some(2),
        avgColLen = Some(1.5))),
      ("c5", BasicColStats(dataType = BooleanType, numNulls = 0, numTrues = Some(1),
        numFalses = Some(1), ndv = Some(2))),
      ("c6", BasicColStats(dataType = DateType, numNulls = 0, max = Some(32), min = Some(0),
        ndv = Some(2))),
      ("c7", BasicColStats(dataType = TimestampType, numNulls = 0, max = Some(5000000),
        min = Some(0), ndv = Some(2))),
      ("c8", BasicColStats(dataType = LongType, numNulls = 0, max = Some(5), min = Some(4),
        ndv = Some(2))))
    checkColStats(rdd, schema, statsSeq)
  }

  test("update table-level stats while collecting column-level stats") {
    val table = "tbl"
    val tmpTable = "tmp"
    withTable(table, tmpTable) {
      val rdd = sparkContext.parallelize(Seq(Row(1)))
      val df = spark.createDataFrame(rdd, StructType(Seq(
        StructField(name = "c1", dataType = IntegerType, nullable = false))))
      df.write.format("json").saveAsTable(tmpTable)

      sql(s"CREATE TABLE $table (c1 int)")
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      val fetchedStats1 = checkTableStats(tableName = table, isDataSourceTable = false,
        hasSizeInBytes = true, expectedRowCounts = Some(1))

      // update table between analyze table and analyze column commands
      sql(s"INSERT INTO $table SELECT * FROM $tmpTable")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats2 = checkTableStats(tableName = table, isDataSourceTable = false,
        hasSizeInBytes = true, expectedRowCounts = Some(2))

      assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)
      val basicColStats = fetchedStats2.get.basicColStats("c1")
      checkColStats(colStats = basicColStats, expectedColStats = BasicColStats(
        dataType = IntegerType, numNulls = 0, max = Some(1), min = Some(1), ndv = Some(1)))
    }
  }

  test("estimates the size of a test MetastoreRelation") {
    val df = sql("""SELECT * FROM src""")
    val sizes = df.queryExecution.analyzed.collect { case mr: MetastoreRelation =>
      mr.statistics.sizeInBytes
    }
    assert(sizes.size === 1, s"Size wrong for:\n ${df.queryExecution}")
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Row],
        ct: ClassTag[_]): Unit = {
      before()

      var df = sql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = df.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.statistics.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold
        && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${df.queryExecution}")

      checkAnswer(df, expectedAnswer) // check correctness of output

      spark.sessionState.conf.settings.synchronized {
        val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1""")
        df = sql(query)
        bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = df.queryExecution.sparkPlan.collect { case j: SortMergeJoinExec => j }
        assert(shj.size === 1,
          "SortMergeJoin should be planned when BroadcastHashJoin is turned off")

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp""")
      }

      after()
    }

    /** Tests for MetastoreRelation */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)(Row(238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[MetastoreRelation]]
    )
  }

  test("auto converts to broadcast left semi join, by size estimate of a relation") {
    val leftSemiJoinQuery =
      """SELECT * FROM src a
        |left semi JOIN src b ON a.key=86 and a.key = b.key""".stripMargin
    val answer = Row(86, "val_86")

    var df = sql(leftSemiJoinQuery)

    // Assert src has a size smaller than the threshold.
    val sizes = df.queryExecution.analyzed.collect {
      case r if implicitly[ClassTag[MetastoreRelation]].runtimeClass
        .isAssignableFrom(r.getClass) =>
        r.statistics.sizeInBytes
    }
    assert(sizes.size === 2 && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold
      && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
      s"query should contain two relations, each of which has size smaller than autoConvertSize")

    // Using `sparkPlan` because for relevant patterns in HashJoin to be
    // matched, other strategies need to be applied.
    var bhj = df.queryExecution.sparkPlan.collect {
      case j: BroadcastHashJoinExec => j
    }
    assert(bhj.size === 1,
      s"actual query plans do not contain broadcast join: ${df.queryExecution}")

    checkAnswer(df, answer) // check correctness of output

    spark.sessionState.conf.settings.synchronized {
      val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1")
      df = sql(leftSemiJoinQuery)
      bhj = df.queryExecution.sparkPlan.collect {
        case j: BroadcastHashJoinExec => j
      }
      assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

      val shj = df.queryExecution.sparkPlan.collect {
        case j: SortMergeJoinExec => j
      }
      assert(shj.size === 1,
        "SortMergeJoinExec should be planned when BroadcastHashJoin is turned off")

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp")
    }

  }
}
