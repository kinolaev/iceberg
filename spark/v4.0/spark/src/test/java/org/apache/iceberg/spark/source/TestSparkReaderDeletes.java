/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.spark.source.SparkSQLExecutionHelper.lastExecutedMetricValue;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.spark.source.metrics.NumDeletes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReaderDeletes extends DeleteReadTests {
  private static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @Parameter(index = 2)
  private boolean vectorized;

  @Parameter(index = 3)
  private PlanningMode planningMode;

  @Parameters(name = "fileFormat = {0}, formatVersion = {1}, vectorized = {2}, planningMode = {3}")
  public static Object[][] parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (int version : TestHelpers.V2_AND_ABOVE) {
      parameters.add(new Object[] {FileFormat.PARQUET, version, false, PlanningMode.DISTRIBUTED});
      parameters.add(new Object[] {FileFormat.PARQUET, version, true, PlanningMode.LOCAL});
      if (version == 2) {
        parameters.add(new Object[] {FileFormat.ORC, version, false, PlanningMode.DISTRIBUTED});
        parameters.add(new Object[] {FileFormat.AVRO, version, false, PlanningMode.LOCAL});
      }
    }
    return parameters.toArray(new Object[0][]);
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.appStateStore.asyncTracking.enable", false)
            .config("spark.ui.liveUpdate.period", 0)
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .config(TestBase.DISABLE_UI)
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  @AfterEach
  @Override
  public void cleanup() throws IOException {
    super.cleanup();
    dropTable("test3");
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Table table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(formatVersion));
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
        .set(TableProperties.DATA_PLANNING_MODE, planningMode.modeName())
        .set(TableProperties.DELETE_PLANNING_MODE, planningMode.modeName())
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .commit();
    if (format.equals(FileFormat.PARQUET) || format.equals(FileFormat.ORC)) {
      String vectorizationEnabled =
          format.equals(FileFormat.PARQUET)
              ? TableProperties.PARQUET_VECTORIZATION_ENABLED
              : TableProperties.ORC_VECTORIZATION_ENABLED;
      String batchSize =
          format.equals(FileFormat.PARQUET)
              ? TableProperties.PARQUET_BATCH_SIZE
              : TableProperties.ORC_BATCH_SIZE;
      table.updateProperties().set(vectorizationEnabled, String.valueOf(vectorized)).commit();
      if (vectorized) {
        // split 7 records to two batches to cover more code paths
        table.updateProperties().set(batchSize, "4").commit();
      }
    }
    return table;
  }

  @Override
  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  protected boolean countDeletes() {
    return true;
  }

  @Override
  protected long deleteCount() {
    return Long.parseLong(lastExecutedMetricValue(spark, NumDeletes.DISPLAY_STRING));
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) {
    return rowSet(name, table.schema().select(columns).asStruct(), columns);
  }

  public StructLikeSet rowSet(String name, Types.StructType projection, String... columns) {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", name).toString())
            .selectExpr(columns);

    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              set.add(rowWrapper.wrap(row));
            });

    return set;
  }

  @TestTemplate
  public void testEqualityDeleteWithFilter() throws IOException {
    String tableName = table.name().substring(table.name().lastIndexOf(".") + 1);
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();

    Types.StructType projection = table.schema().select("*").asStruct();
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .filter("data = 'a'") // select a deleted row
            .selectExpr("*");

    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actual.add(rowWrapper.wrap(row));
            });

    assertThat(actual).as("Table should contain no rows").hasSize(0);
  }

  // creates a table directly through the catalog with the given spec actually applied (unlike
  // createTable(), which silently ignores the requested spec and always creates an unpartitioned
  // table)
  private Table createPartitionedTable(String name, Schema schema, PartitionSpec spec) {
    Table t = catalog.createTable(TableIdentifier.of("default", name), schema, spec);
    TableOperations ops = ((BaseTable) t).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(formatVersion));
    t.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format.name()).commit();
    return t;
  }

  private OutputFile newOutputFile() throws IOException {
    return Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
  }

  private Set<Integer> idsOf(Table t, String name) {
    Dataset<Row> df = spark.read().format("iceberg").load(TableIdentifier.of("default", name).toString());
    return df.collectAsList().stream().map(r -> r.getInt(r.fieldIndex("id"))).collect(Collectors.toSet());
  }

  @TestTemplate
  public void testEqualityDeleteScopedToPartition() throws IOException {
    // a normal read does not preserve partition grouping by default (see
    // read.split.preserve-data-grouping), so a scan can plan data files from different
    // partitions into the same task group. an equality delete written for one partition must
    // not affect a different partition's data file, even if that file contains a row with the
    // same equality-delete key.
    String name = "test_partition_scope";
    Table t = createPartitionedTable(name, DATE_SCHEMA, DATE_SPEC);

    try {
      GenericRecord record = GenericRecord.create(t.schema());
      Record row1 = record.copy("dt", LocalDate.parse("2021-09-01"), "data", "a", "id", 1);
      Record row2 = record.copy("dt", LocalDate.parse("2021-09-02"), "data", "a", "id", 2);

      DataFile dataFile1 =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
              Lists.newArrayList(row1));
      DataFile dataFile2 =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-02"))),
              Lists.newArrayList(row2));
      t.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

      // delete "data" = "a", scoped only to the dt=2021-09-02 partition; row1, which shares the
      // same key in a different partition, must be unaffected
      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile eqDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-02"))),
              Lists.newArrayList(dataDelete.copy("data", "a")),
              deleteRowSchema);
      t.newRowDelta().addDeletes(eqDelete).commit();

      // confirm the premise this test depends on: both partitions' data files are actually
      // planned into a single task group, rather than relying on the current default never
      // changing or the two tiny files happening to co-locate
      try (CloseableIterable<CombinedScanTask> tasks = t.newScan().planTasks()) {
        List<CombinedScanTask> taskList = Lists.newArrayList(tasks);
        assertThat(taskList)
            .as("Both partitions' data files must be planned into a single task group")
            .hasSize(1);
        assertThat(Lists.newArrayList(taskList.get(0).files()))
            .as("The single task group must contain both data files")
            .hasSize(2);
      }

      assertThat(idsOf(t, name))
          .as("Row in a different partition sharing the delete key must not be removed")
          .containsExactly(1);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testGlobalEqualityDeleteAppliesAcrossPartitions() throws IOException {
    // an equality delete written for an unpartitioned spec is "global" and must apply to data
    // files in every partition, unlike a delete written for a partitioned spec (see
    // testEqualityDeleteScopedToPartition)
    String name = "test_global_delete";
    Table t = createPartitionedTable(name, DATE_SCHEMA, PartitionSpec.unpartitioned());

    try {
      // write the delete while the table's only spec is still unpartitioned, so it is global
      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile globalDelete =
          FileHelpers.writeDeleteFile(
              t, newOutputFile(), null, Lists.newArrayList(dataDelete.copy("data", "a")), deleteRowSchema);

      // evolve the spec to partition by day
      t.updateSpec().addField(Expressions.day("dt")).commit();

      GenericRecord record = GenericRecord.create(t.schema());
      Record row1 = record.copy("dt", LocalDate.parse("2021-09-01"), "data", "a", "id", 1);
      Record row2 = record.copy("dt", LocalDate.parse("2021-09-02"), "data", "a", "id", 2);
      DataFile dataFile1 =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
              Lists.newArrayList(row1));
      DataFile dataFile2 =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-02"))),
              Lists.newArrayList(row2));
      t.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

      // commit the (already-written) global delete last, so its sequence number is greater than
      // both data files'
      t.newRowDelta().addDeletes(globalDelete).commit();

      assertThat(idsOf(t, name))
          .as("Global delete must remove the matching row in every partition")
          .isEmpty();
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testGlobalAndPartitionScopedDeletesBothApplyToSameFile() throws IOException {
    // the per-file delete filter is built from eqDeletesCaches, a list of independent maps (one
    // for global deletes, one for this file's partition); this exercises a single data file that
    // must consult both at once, rather than only one or the other
    String name = "test_global_and_partition_delete";
    Table t = createPartitionedTable(name, DATE_SCHEMA, PartitionSpec.unpartitioned());

    try {
      // write the global delete while the table's only spec is still unpartitioned
      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile globalDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              null,
              Lists.newArrayList(dataDelete.copy("data", "a")),
              deleteRowSchema);

      // evolve the spec to partition by day
      t.updateSpec().addField(Expressions.day("dt")).commit();

      LocalDate dt = LocalDate.parse("2021-09-01");
      GenericRecord record = GenericRecord.create(t.schema());
      Record rowA = record.copy("dt", dt, "data", "a", "id", 1); // removed by the global delete
      Record rowB = record.copy("dt", dt, "data", "b", "id", 2); // removed by the partition delete
      Record rowC = record.copy("dt", dt, "data", "c", "id", 3); // survives both
      DataFile dataFile =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(dt)),
              Lists.newArrayList(rowA, rowB, rowC));
      t.newAppend().appendFile(dataFile).commit();

      // a delete scoped to the same partition as the data file, targeting a different key
      DeleteFile partitionDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(dt)),
              Lists.newArrayList(dataDelete.copy("data", "b")),
              deleteRowSchema);
      t.newRowDelta().addDeletes(partitionDelete).commit();

      // commit the (already-written) global delete last, so its sequence number is also greater
      // than the data file's
      t.newRowDelta().addDeletes(globalDelete).commit();

      assertThat(idsOf(t, name))
          .as("Only the row matching neither the global nor the partition-scoped delete survives")
          .containsExactly(3);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testMultipleEqualityFieldIdGroups() throws IOException {
    // equality deletes are grouped by their set of equality field IDs, and the constructor
    // iterates every group in every cache map; this exercises two delete files that use two
    // different equality columns against the same data file
    String name = "test_multiple_eq_field_groups";
    Table t = createPartitionedTable(name, SCHEMA, PartitionSpec.unpartitioned());

    try {
      GenericRecord record = GenericRecord.create(t.schema());
      Record row1 = record.copy("id", 1, "data", "a");
      Record row2 = record.copy("id", 2, "data", "b");
      Record row3 = record.copy("id", 3, "data", "c");
      DataFile dataFile =
          FileHelpers.writeDataFile(
              t, newOutputFile(), null, Lists.newArrayList(row1, row2, row3));
      t.newAppend().appendFile(dataFile).commit();

      // delete keyed by "data" removes id=1
      Schema dataDeleteSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(dataDeleteSchema);
      DeleteFile deleteByData =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              null,
              Lists.newArrayList(dataDelete.copy("data", "a")),
              dataDeleteSchema);

      // delete keyed by "id" removes id=3 — a different equality field ID set than above
      Schema idDeleteSchema = t.schema().select("id");
      Record idDelete = GenericRecord.create(idDeleteSchema);
      DeleteFile deleteById =
          FileHelpers.writeDeleteFile(
              t, newOutputFile(), null, Lists.newArrayList(idDelete.copy("id", 3)), idDeleteSchema);

      t.newRowDelta().addDeletes(deleteByData).addDeletes(deleteById).commit();

      assertThat(idsOf(t, name))
          .as("Rows matching either equality-field-id group's delete must be removed")
          .containsExactly(2);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testPartitionScopedEqualityDeleteSurvivesPartitionEvolution() throws IOException {
    // a partition-scoped equality delete is keyed by the exact (specId, partition) of the
    // delete file, mirroring core's DeleteFileIndex (see EqualityDeletes/PartitionMap in
    // DeleteFileIndex, which key and look up strictly by the data file's own specId — there is no
    // cross-spec normalization there either). this must hold across partition evolution: the
    // delete must remain effective for data files under its original spec/partition, and must not
    // bleed into data files written under a newer spec, even if they cover an overlapping range
    // of the same underlying column.
    String name = "test_partition_evolution";
    Table t = createPartitionedTable(name, DATE_SCHEMA, DATE_SPEC);

    try {
      GenericRecord record = GenericRecord.create(t.schema());
      Record oldRow = record.copy("dt", LocalDate.parse("2021-09-01"), "data", "a", "id", 1);
      DataFile oldDataFile =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
              Lists.newArrayList(oldRow));
      t.newAppend().appendFile(oldDataFile).commit();

      // a partition-scoped delete under the original (day-partitioned) spec
      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile oldDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
              Lists.newArrayList(dataDelete.copy("data", "a")),
              deleteRowSchema);
      t.newRowDelta().addDeletes(oldDelete).commit();

      // evolve the spec to a different transform (month instead of day) on the same column
      t.updateSpec().removeField("dt_day").addField(Expressions.month("dt")).commit();

      // a new data file under the new spec, in the month containing 2021-09-01, sharing the same
      // equality-delete key
      Record newRow = record.copy("dt", LocalDate.parse("2021-09-15"), "data", "a", "id", 2);
      DataFile newDataFile =
          FileHelpers.writeDataFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(
                  DateTimeUtil.daysToMonths(
                      DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-15")))),
              Lists.newArrayList(newRow));
      t.newAppend().appendFile(newDataFile).commit();

      assertThat(idsOf(t, name))
          .as(
              "Delete must still remove id=1 under its original spec/partition, and must not "
                  + "carry over to id=2 under the newly evolved spec")
          .containsExactly(2);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testEqualityDeleteSameSequenceNumberDoesNotApply() throws IOException {
    // see https://github.com/apache/iceberg/issues/15305 — an equality delete must not apply to
    // a data file with the same (not strictly greater) data sequence number
    String name = "test_same_sequence";
    Table t = createPartitionedTable(name, SCHEMA, PartitionSpec.unpartitioned());

    try {
      GenericRecord record = GenericRecord.create(t.schema());
      Record row = record.copy("id", 1, "data", "x");
      DataFile dataFile =
          FileHelpers.writeDataFile(t, newOutputFile(), null, Lists.newArrayList(row));

      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile sameSeqDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              null,
              Lists.newArrayList(dataDelete.copy("data", "x")),
              deleteRowSchema);

      // add the data file and the delete in the same commit, so they share a data sequence
      // number; the delete must not apply since it is not strictly newer
      t.newRowDelta().addRows(dataFile).addDeletes(sameSeqDelete).commit();

      assertThat(idsOf(t, name))
          .as("Row must survive a delete with an equal (not strictly greater) sequence number")
          .containsExactly(1);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testReinsertedRowSurvivesEqualityDelete() throws IOException {
    // a row re-inserted after an equality delete, with a data sequence number greater than the
    // delete's, must survive — the delete only applies to data strictly older than itself
    String name = "test_reinsert";
    Table t = createPartitionedTable(name, SCHEMA, PartitionSpec.unpartitioned());

    try {
      GenericRecord record = GenericRecord.create(t.schema());
      Record original = record.copy("id", 1, "data", "x");
      DataFile dataFile =
          FileHelpers.writeDataFile(t, newOutputFile(), null, Lists.newArrayList(original));
      t.newAppend().appendFile(dataFile).commit();

      Schema deleteRowSchema = t.schema().select("data");
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile eqDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              null,
              Lists.newArrayList(dataDelete.copy("data", "x")),
              deleteRowSchema);
      t.newRowDelta().addDeletes(eqDelete).commit();

      // re-insert a row with the same key, in a later commit (a higher sequence number than the
      // delete)
      Record reinserted = record.copy("id", 2, "data", "x");
      DataFile reinsertedFile =
          FileHelpers.writeDataFile(t, newOutputFile(), null, Lists.newArrayList(reinserted));
      t.newAppend().appendFile(reinsertedFile).commit();

      assertThat(idsOf(t, name))
          .as("Row re-inserted after the delete must survive")
          .containsExactly(2);
    } finally {
      catalog.dropTable(TableIdentifier.of("default", name));
    }
  }

  @TestTemplate
  public void testEqualityDeleteFuzzEquivalence() throws IOException {
    // compare the new (pooled, partition- and sequence-scoped) Spark read path against
    // IcebergGenerics, which builds an unshared, unpooled DeleteFilter per data file and is
    // therefore a trustworthy oracle, across many randomized tables with cross-partition primary
    // key collisions, a mix of global and partition-scoped deletes, and some deletes committed in
    // the same RowDelta as their data file (same sequence number, must not apply). only run once
    // per test class invocation; the fuzz logic itself does not depend on file format,
    // vectorization, or planning mode.
    assumeThat(format).isEqualTo(FileFormat.PARQUET);
    assumeThat(vectorized).isFalse();

    for (int seed = 0; seed < 20; seed++) {
      String name = "test_fuzz_" + seed;
      Table t = createPartitionedTable(name, DATE_SCHEMA, PartitionSpec.unpartitioned());
      try {
        runFuzzTrial(t, name, seed);
      } finally {
        catalog.dropTable(TableIdentifier.of("default", name));
      }
    }
  }

  private void runFuzzTrial(Table t, String name, long seed) throws IOException {
    Random random = new Random(seed);
    int idPoolSize = 6; // small pool so ids collide across partitions
    Schema deleteRowSchema = t.schema().select("data");

    // write 0-2 global deletes while the table's only spec is still unpartitioned; committed at
    // the very end so they end up newer than all the data below and therefore actually apply
    List<DeleteFile> pendingGlobalDeletes = Lists.newArrayList();
    for (int i = 0; i < random.nextInt(3); i++) {
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      String key = String.valueOf(random.nextInt(idPoolSize));
      pendingGlobalDeletes.add(
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              null,
              Lists.newArrayList(dataDelete.copy("data", key)),
              deleteRowSchema));
    }

    // evolve to a day-partitioned spec for the rest of the trial
    t.updateSpec().addField(Expressions.day("dt")).commit();

    int numPartitions = 3 + random.nextInt(3); // 3-5
    int nextId = 0;

    for (int p = 0; p < numPartitions; p++) {
      LocalDate dt = LocalDate.parse("2021-09-01").plusDays(p);
      int numFiles = 1 + random.nextInt(3); // 1-3 data files per partition
      List<Integer> partitionIds = Lists.newArrayList();

      for (int f = 0; f < numFiles; f++) {
        int numRows = 1 + random.nextInt(4); // 1-4 rows per file
        List<Record> rows = Lists.newArrayList();
        GenericRecord record = GenericRecord.create(t.schema());
        for (int r = 0; r < numRows; r++) {
          int id = nextId++;
          partitionIds.add(id);
          rows.add(record.copy("dt", dt, "data", String.valueOf(id % idPoolSize), "id", id));
        }

        DataFile dataFile =
            FileHelpers.writeDataFile(
                t, newOutputFile(), TestHelpers.Row.of(DateTimeUtil.daysFromDate(dt)), rows);

        // occasionally add a delete scoped to this partition for a key already written to it
        if (random.nextBoolean() && !partitionIds.isEmpty()) {
          String key =
              String.valueOf(partitionIds.get(random.nextInt(partitionIds.size())) % idPoolSize);
          Record dataDelete = GenericRecord.create(deleteRowSchema);
          DeleteFile partitionDelete =
              FileHelpers.writeDeleteFile(
                  t,
                  newOutputFile(),
                  TestHelpers.Row.of(DateTimeUtil.daysFromDate(dt)),
                  Lists.newArrayList(dataDelete.copy("data", key)),
                  deleteRowSchema);

          if (random.nextBoolean()) {
            // commit in the same RowDelta as the data file: same sequence number, must not apply
            t.newRowDelta().addRows(dataFile).addDeletes(partitionDelete).commit();
          } else {
            t.newAppend().appendFile(dataFile).commit();
            t.newRowDelta().addDeletes(partitionDelete).commit();
          }
        } else {
          t.newAppend().appendFile(dataFile).commit();
        }
      }
    }

    // a handful of deletes for keys chosen across the whole id pool, each scoped to a randomly
    // picked existing partition, committed after all the data above so they are newer than it
    for (int i = 0; i < 3; i++) {
      LocalDate dt = LocalDate.parse("2021-09-01").plusDays(random.nextInt(numPartitions));
      String key = String.valueOf(random.nextInt(idPoolSize));
      Record dataDelete = GenericRecord.create(deleteRowSchema);
      DeleteFile partitionDelete =
          FileHelpers.writeDeleteFile(
              t,
              newOutputFile(),
              TestHelpers.Row.of(DateTimeUtil.daysFromDate(dt)),
              Lists.newArrayList(dataDelete.copy("data", key)),
              deleteRowSchema);
      t.newRowDelta().addDeletes(partitionDelete).commit();
    }

    // commit the pre-written global deletes last, so they are newer than all the data and
    // partition-scoped deletes above and therefore actually apply
    for (DeleteFile globalDelete : pendingGlobalDeletes) {
      t.newRowDelta().addDeletes(globalDelete).commit();
    }

    Set<Integer> expected = Sets.newHashSet();
    try (CloseableIterable<Record> records = IcebergGenerics.read(t).build()) {
      for (Record record : records) {
        expected.add((Integer) record.getField("id"));
      }
    }

    assertThat(idsOf(t, name))
        .as("seed=%s: Spark read result must match the unpooled IcebergGenerics oracle", seed)
        .isEqualTo(expected);
  }

  @TestTemplate
  public void testReadEqualityDeleteRows() throws IOException {
    Schema deleteSchema1 = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteSchema1);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d") // id = 89
            );

    Schema deleteSchema2 = table.schema().select("id");
    Record idDelete = GenericRecord.create(deleteSchema2);
    List<Record> idDeletes =
        Lists.newArrayList(
            idDelete.copy("id", 121), // id = 121
            idDelete.copy("id", 122) // id = 122
            );

    DeleteFile eqDelete1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteSchema1);

    DeleteFile eqDelete2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            idDeletes,
            deleteSchema2);

    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).commit();

    StructLikeSet expectedRowSet = rowSetWithIds(29, 89, 121, 122);

    Types.StructType type = table.schema().asStruct();
    StructLikeSet actualRowSet = StructLikeSet.create(type);

    CloseableIterable<CombinedScanTask> tasks =
        TableScanUtil.planTasks(
            table.newScan().planFiles(),
            TableProperties.METADATA_SPLIT_SIZE_DEFAULT,
            TableProperties.SPLIT_LOOKBACK_DEFAULT,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    for (CombinedScanTask task : tasks) {
      try (EqualityDeleteRowReader reader =
          new EqualityDeleteRowReader(
              task, table, table.io(), table.schema(), table.schema(), false, true)) {
        while (reader.next()) {
          actualRowSet.add(
              new InternalRowWrapper(
                      SparkSchemaUtil.convert(table.schema()), table.schema().asStruct())
                  .wrap(reader.get().copy()));
        }
      }
    }

    assertThat(actualRowSet).as("should include 4 deleted row").hasSize(4);
    assertThat(actualRowSet).as("deleted row should be matched").isEqualTo(expectedRowSet);
  }

  @TestTemplate
  public void testPosDeletesAllRowsInBatch() throws IOException {
    // read.parquet.vectorization.batch-size is set to 4, so the 4 rows in the first batch are all
    // deleted.
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 0L), // id = 29
            Pair.of(dataFile.location(), 1L), // id = 43
            Pair.of(dataFile.location(), 2L), // id = 61
            Pair.of(dataFile.location(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes,
            formatVersion);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 43, 61, 89);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testPosDeletesWithDeletedColumn() throws IOException {
    // read.parquet.vectorization.batch-size is set to 4, so the 4 rows in the first batch are all
    // deleted.
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 0L), // id = 29
            Pair.of(dataFile.location(), 1L), // id = 43
            Pair.of(dataFile.location(), 2L), // id = 61
            Pair.of(dataFile.location(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes,
            formatVersion);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSet(29, 43, 61, 89);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testEqualityDeleteWithDeletedColumn() throws IOException {
    String tableName = table.name().substring(table.name().lastIndexOf(".") + 1);
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = expectedRowSet(29, 89, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testMixedPosAndEqDeletesWithDeletedColumn() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            dataSchema);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 3L), // id = 89
            Pair.of(dataFile.location(), 5L) // id = 121
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes,
            formatVersion);

    table
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSet(29, 89, 121, 122);
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testFilterOnDeletedMetadataColumn() throws IOException {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 0L), // id = 29
            Pair.of(dataFile.location(), 1L), // id = 43
            Pair.of(dataFile.location(), 2L), // id = 61
            Pair.of(dataFile.location(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes,
            formatVersion);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = expectedRowSetWithNonDeletesOnly(29, 43, 61, 89);

    // get non-deleted rows
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .select("id", "data", "_deleted")
            .filter("_deleted = false");

    Types.StructType projection = PROJECTION_SCHEMA.asStruct();
    StructLikeSet actual = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actual.add(rowWrapper.wrap(row));
            });

    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);

    StructLikeSet expectedDeleted = expectedRowSetWithDeletesOnly(29, 43, 61, 89);

    // get deleted rows
    df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            .select("id", "data", "_deleted")
            .filter("_deleted = true");

    StructLikeSet actualDeleted = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(projection);
              actualDeleted.add(rowWrapper.wrap(row));
            });

    assertThat(actualDeleted).as("Table should contain expected row").isEqualTo(expectedDeleted);
  }

  @TestTemplate
  public void testIsDeletedColumnWithoutDeleteFile() {
    StructLikeSet expected = expectedRowSet();
    StructLikeSet actual =
        rowSet(tableName, PROJECTION_SCHEMA.asStruct(), "id", "data", "_deleted");
    assertThat(actual).as("Table should contain expected row").isEqualTo(expected);
    checkDeleteCount(0L);
  }

  @TestTemplate
  public void testPosDeletesOnParquetFileWithMultipleRowGroups() throws IOException {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);

    String tblName = "test3";
    Table tbl = createTable(tblName, SCHEMA, PartitionSpec.unpartitioned());

    List<Path> fileSplits = Lists.newArrayList();
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    Configuration conf = new Configuration();
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();
    Path testFilePath = new Path(testFile.getAbsolutePath());

    // Write a Parquet file with more than one row group
    ParquetFileWriter parquetFileWriter =
        new ParquetFileWriter(conf, ParquetSchemaUtil.convert(SCHEMA, "test3Schema"), testFilePath);
    parquetFileWriter.start();
    for (int i = 0; i < 2; i += 1) {
      File split = File.createTempFile("junit", null, temp.toFile());
      assertThat(split.delete()).as("Delete should succeed").isTrue();
      Path splitPath = new Path(split.getAbsolutePath());
      fileSplits.add(splitPath);
      try (FileAppender<InternalRow> writer =
          Parquet.write(Files.localOutput(split))
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(sparkSchema, msgType))
              .schema(SCHEMA)
              .overwrite()
              .build()) {
        Iterable<InternalRow> records = RandomData.generateSpark(SCHEMA, 100, 34 * i + 37);
        writer.addAll(records);
      }
      parquetFileWriter.appendFile(
          org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(splitPath, conf));
    }
    parquetFileWriter.end(
        ParquetFileWriter.mergeMetadataFiles(fileSplits, conf)
            .getFileMetaData()
            .getKeyValueMetaData());

    // Add the file to the table
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(org.apache.iceberg.hadoop.HadoopInputFile.fromPath(testFilePath, conf))
            .withFormat("parquet")
            .withRecordCount(200)
            .build();
    tbl.newAppend().appendFile(dataFile).commit();

    // Add positional deletes to the table
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 97L),
            Pair.of(dataFile.location(), 98L),
            Pair.of(dataFile.location(), 99L),
            Pair.of(dataFile.location(), 101L),
            Pair.of(dataFile.location(), 103L),
            Pair.of(dataFile.location(), 107L),
            Pair.of(dataFile.location(), 109L));
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            deletes,
            formatVersion);
    tbl.newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    assertThat(rowSet(tblName, tbl, "*")).hasSize(193);
  }

  @TestTemplate
  public void testEqualityDeleteWithDifferentScanAndDeleteColumns() throws IOException {
    assumeThat(format).isEqualTo(FileFormat.PARQUET);
    initDateTable();

    Schema deleteRowSchema = dateTable.schema().select("dt");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("dt", LocalDate.parse("2021-09-01")),
            dataDelete.copy("dt", LocalDate.parse("2021-09-02")),
            dataDelete.copy("dt", LocalDate.parse("2021-09-03")));

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            dateTable,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes.subList(0, 3),
            deleteRowSchema);

    dateTable.newRowDelta().addDeletes(eqDeletes).commit();

    CloseableIterable<CombinedScanTask> tasks =
        TableScanUtil.planTasks(
            dateTable.newScan().planFiles(),
            TableProperties.METADATA_SPLIT_SIZE_DEFAULT,
            TableProperties.SPLIT_LOOKBACK_DEFAULT,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

    ParquetBatchReadConf conf = ImmutableParquetBatchReadConf.builder().batchSize(7).build();

    for (CombinedScanTask task : tasks) {
      try (BatchDataReader reader =
          new BatchDataReader(
              // expected column is id, while the equality filter column is dt
              dateTable,
              dateTable.io(),
              task,
              dateTable.schema(),
              dateTable.schema().select("id"),
              false,
              conf,
              null,
              true)) {
        while (reader.next()) {
          ColumnarBatch columnarBatch = reader.get();
          int numOfCols = columnarBatch.numCols();
          assertThat(numOfCols).as("Number of columns").isEqualTo(1);
          assertThat(columnarBatch.column(0).dataType()).as("Column type").isEqualTo(IntegerType);
        }
      }
    }
  }

  private static final Schema PROJECTION_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          MetadataColumns.IS_DELETED);

  private static StructLikeSet expectedRowSet(int... idsToRemove) {
    return expectedRowSet(false, false, idsToRemove);
  }

  private static StructLikeSet expectedRowSetWithDeletesOnly(int... idsToRemove) {
    return expectedRowSet(false, true, idsToRemove);
  }

  private static StructLikeSet expectedRowSetWithNonDeletesOnly(int... idsToRemove) {
    return expectedRowSet(true, false, idsToRemove);
  }

  private static StructLikeSet expectedRowSet(
      boolean removeDeleted, boolean removeNonDeleted, int... idsToRemove) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRemove));
    List<Record> records = recordsWithDeletedColumn();
    // mark rows deleted
    records.forEach(
        record -> {
          if (deletedIds.contains(record.getField("id"))) {
            record.setField(MetadataColumns.IS_DELETED.name(), true);
          }
        });

    records.removeIf(record -> deletedIds.contains(record.getField("id")) && removeDeleted);
    records.removeIf(record -> !deletedIds.contains(record.getField("id")) && removeNonDeleted);

    StructLikeSet set = StructLikeSet.create(PROJECTION_SCHEMA.asStruct());
    records.forEach(
        record -> set.add(new InternalRecordWrapper(PROJECTION_SCHEMA.asStruct()).wrap(record)));

    return set;
  }

  @Nonnull
  private static List recordsWithDeletedColumn() {
    List records = Lists.newArrayList();

    // records all use IDs that are in bucket id_bucket=0
    GenericRecord record = GenericRecord.create(PROJECTION_SCHEMA);
    records.add(record.copy("id", 29, "data", "a", "_deleted", false));
    records.add(record.copy("id", 43, "data", "b", "_deleted", false));
    records.add(record.copy("id", 61, "data", "c", "_deleted", false));
    records.add(record.copy("id", 89, "data", "d", "_deleted", false));
    records.add(record.copy("id", 100, "data", "e", "_deleted", false));
    records.add(record.copy("id", 121, "data", "f", "_deleted", false));
    records.add(record.copy("id", 122, "data", "g", "_deleted", false));
    return records;
  }
}
