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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchDataReader extends BaseBatchReader<FileScanTask>
    implements PartitionReader<ColumnarBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchDataReader.class);

  private final long numSplits;

  // equality deletes written for an unpartitioned spec apply to every data file in the table,
  // while deletes written for a partitioned spec only apply to data files in the same partition.
  // these are tracked (and their content loaded) separately so that sharing the loading of
  // equality deletes across the data files in a scan task group never pools deletes across
  // partitions, which would cause deletes from one partition to incorrectly apply to another.
  private final List<DeleteFile> globalEqDeletes;
  private final Map<Set<Integer>, StructLikeMap<Long>> globalEqDeletesCache;
  private final PartitionMap<List<DeleteFile>> eqDeletesByPartition;
  private final PartitionMap<Map<Set<Integer>, StructLikeMap<Long>>> eqDeletesCacheByPartition;

  BatchDataReader(
      SparkInputPartition partition,
      ParquetBatchReadConf parquetBatchReadConf,
      OrcBatchReadConf orcBatchReadConf) {
    this(
        partition.table(),
        partition.io(),
        partition.taskGroup(),
        SnapshotUtil.schemaFor(partition.table(), partition.branch()),
        partition.expectedSchema(),
        partition.isCaseSensitive(),
        parquetBatchReadConf,
        orcBatchReadConf,
        partition.cacheDeleteFilesOnExecutors());
  }

  BatchDataReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive,
      ParquetBatchReadConf parquetConf,
      OrcBatchReadConf orcConf,
      boolean cacheDeleteFilesOnExecutors) {
    super(
        table,
        fileIO,
        taskGroup,
        tableSchema,
        expectedSchema,
        caseSensitive,
        parquetConf,
        orcConf,
        cacheDeleteFilesOnExecutors);

    numSplits = taskGroup.tasks().size();
    LOG.debug("Reading {} file split(s) for table {}", numSplits, table.name());

    // load the equality deletes referenced by this task group once, so that those shared across
    // multiple data files in the group don't get loaded again for each data file
    Map<Integer, PartitionSpec> specs = table.specs();
    List<DeleteFile> globalDeletes = Lists.newArrayList();
    PartitionMap<List<DeleteFile>> partitionDeletes = PartitionMap.create(specs);
    Set<String> seenEqDeletePaths = Sets.newHashSet();
    for (FileScanTask task : taskGroup.tasks()) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() != FileContent.EQUALITY_DELETES
            || (delete.location() != null && !seenEqDeletePaths.add(delete.location()))) {
          continue;
        }

        if (specs.get(delete.specId()).isUnpartitioned()) {
          globalDeletes.add(delete);
        } else {
          partitionDeletes
              .computeIfAbsent(delete.specId(), delete.partition(), Lists::newArrayList)
              .add(delete);
        }
      }
    }

    this.globalEqDeletes = globalDeletes;
    this.globalEqDeletesCache =
        globalDeletes.isEmpty()
            ? ImmutableMap.of()
            : new SparkDeleteFilter(null, globalDeletes, counter(), false).loadEqualityDeletes();

    this.eqDeletesByPartition = partitionDeletes;
    this.eqDeletesCacheByPartition = PartitionMap.create(specs);
    for (Map.Entry<Pair<Integer, StructLike>, List<DeleteFile>> entry :
        partitionDeletes.entrySet()) {
      Map<Set<Integer>, StructLikeMap<Long>> cache =
          new SparkDeleteFilter(null, entry.getValue(), counter(), false).loadEqualityDeletes();
      eqDeletesCacheByPartition.put(entry.getKey().first(), entry.getKey().second(), cache);
    }
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[] {
      new TaskNumSplits(numSplits), new TaskNumDeletes(counter().get())
    };
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
    return Stream.concat(Stream.of(task.file()), task.deletes().stream());
  }

  @Override
  protected CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    String filePath = task.file().location();
    LOG.debug("Opening data file {}", filePath);

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile inputFile = getInputFile(filePath);
    Preconditions.checkNotNull(inputFile, "Could not find InputFile associated with FileScanTask");

    DataFile file = task.file();
    List<DeleteFile> partitionEqDeletes = eqDeletesByPartition.get(file.specId(), file.partition());
    if (partitionEqDeletes == null) {
      partitionEqDeletes = ImmutableList.of();
    }

    Stream<DeleteFile> posDeletes =
        task.deletes().stream().filter(d -> d.content() == FileContent.POSITION_DELETES);
    List<DeleteFile> deletes =
        Stream.concat(
                posDeletes, Stream.concat(globalEqDeletes.stream(), partitionEqDeletes.stream()))
            .toList();

    Map<Set<Integer>, StructLikeMap<Long>> partitionEqDeletesCache =
        eqDeletesCacheByPartition.get(file.specId(), file.partition());
    List<Map<Set<Integer>, StructLikeMap<Long>>> eqDeletesCaches =
        partitionEqDeletesCache != null
            ? ImmutableList.of(globalEqDeletesCache, partitionEqDeletesCache)
            : ImmutableList.of(globalEqDeletesCache);

    // dataSequenceNumber() can be null, e.g. while reading old v1/v2 manifests; default to 0 so
    // any real delete still applies
    Long dataSequenceNumber = file.dataSequenceNumber();
    long fileSeq = dataSequenceNumber != null ? dataSequenceNumber : 0L;
    SparkDeleteFilter deleteFilter =
        new SparkDeleteFilter(filePath, deletes, counter(), true, eqDeletesCaches, fileSeq);

    Map<Integer, ?> idToConstant = constantsMap(task, deleteFilter.requiredSchema());

    return newBatchIterable(
            inputFile,
            task.file().format(),
            task.start(),
            task.length(),
            task.residual(),
            idToConstant,
            deleteFilter)
        .iterator();
  }
}
