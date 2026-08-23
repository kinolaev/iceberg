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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParallelPositionDeleteCacheLoader {

  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();

  @TempDir private File temp;

  // tiny row-group/page sizes so a handful of paths already spans multiple row groups and
  // multiple pages per row group, giving a mix of homogeneous and mixed pages without needing
  // huge data
  private Map<String, String> writeProperties() {
    return ImmutableMap.<String, String>builder()
        .put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "2048")
        .put(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
        .put(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1")
        .put(TableProperties.PARQUET_PAGE_ROW_LIMIT, "5")
        .put(
            TableProperties.PARQUET_COMPRESSION,
            TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0)
        .put("parquet.columnindex.truncate.length", "256")
        .build();
  }

  private DeleteFile writeDeleteFile(
      Map<String, String> properties, List<String> paths, int positionsPerPath) throws IOException {
    File file = new File(temp, "deletes-" + System.nanoTime() + ".parquet");
    OutputFile out = Files.localOutput(file);
    PositionDeleteWriter<Void> writer =
        Parquet.writeDeletes(out)
            .withSpec(PartitionSpec.unpartitioned())
            .setAll(properties)
            .overwrite()
            .buildPositionWriter();

    PositionDelete<Void> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<Void> closeableWriter = writer) {
      // sorted by (path, pos), matching what a real position delete file guarantees
      for (String path : paths) {
        for (int pos = 0; pos < positionsPerPath; pos++) {
          closeableWriter.write(posDelete.set(path, pos * 2L));
        }
      }
    }
    return writer.toDeleteFile();
  }

  // ground truth: reads every record in the file and lets Deletes.toPositionIndexes do its own
  // (already-validated, production) per-path grouping - no dependency on the code under test
  private CharSequenceMap<PositionDeleteIndex> reference(DeleteFile deleteFile) throws IOException {
    try (CloseableIterable<Record> deletes =
        Parquet.read(Files.localInput(deleteFile.location()))
            .project(POS_DELETE_SCHEMA)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(POS_DELETE_SCHEMA, fileSchema))
            .reuseContainers()
            .build()) {
      return Deletes.toPositionIndexes(deletes, deleteFile);
    }
  }

  private void assertMatches(
      CharSequenceMap<PositionDeleteIndex> actual, CharSequenceMap<PositionDeleteIndex> expected) {
    assertThat(actual.keySet()).as("distinct paths").isEqualTo(expected.keySet());
    for (Map.Entry<CharSequence, PositionDeleteIndex> entry : expected.entrySet()) {
      CharSequence path = entry.getKey();
      PositionDeleteIndex expectedIndex = entry.getValue();
      PositionDeleteIndex actualIndex = actual.get(path);
      assertThat(actualIndex).as("index for %s", path).isNotNull();
      assertThat(actualIndex.cardinality())
          .as("cardinality for %s", path)
          .isEqualTo(expectedIndex.cardinality());
      List<Long> missing = Lists.newArrayList();
      expectedIndex.forEach(
          pos -> {
            if (!actualIndex.isDeleted(pos)) {
              missing.add(pos);
            }
          });
      assertThat(missing).as("positions missing for %s", path).isEmpty();
    }
  }

  @Test
  void multiRowGroupManyPaths() throws IOException {
    List<String> paths =
        List.of(
            "s3://bucket/data/0000000-a.parquet",
            "s3://bucket/data/0000001-b.parquet",
            "s3://bucket/data/0000002-c.parquet",
            "s3://bucket/data/0000003-d.parquet",
            "s3://bucket/data/0000004-e.parquet",
            "s3://bucket/data/0000005-f.parquet");
    DeleteFile deleteFile = writeDeleteFile(writeProperties(), paths, 20);
    InputFile inputFile = Files.localInput(deleteFile.location());

    CharSequenceMap<PositionDeleteIndex> result =
        ParallelPositionDeleteCacheLoader.load(inputFile, deleteFile, 4);
    assertMatches(result, reference(deleteFile));
  }

  // one big row group so every path's data lives in a single row group - some paths span many
  // whole (homogeneous) pages on their own, others share pages with their neighbors (mixed)
  @Test
  void singleRowGroupManyPaths() throws IOException {
    List<String> paths = new java.util.ArrayList<>();
    for (int i = 0; i < 20; i++) {
      paths.add(String.format("s3://bucket/data/%07d-data.parquet", i));
    }
    Map<String, String> properties = new java.util.HashMap<>(writeProperties());
    properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1000000000");
    DeleteFile deleteFile = writeDeleteFile(properties, paths, 200);
    InputFile inputFile = Files.localInput(deleteFile.location());

    CharSequenceMap<PositionDeleteIndex> result =
        ParallelPositionDeleteCacheLoader.load(inputFile, deleteFile, 4);
    assertMatches(result, reference(deleteFile));
  }

  // many tiny paths (1-3 positions each) packed into small pages - forces pages with more than one
  // path transition, exercising the multi-transition-within-one-mixed-page case
  @Test
  void manyTinyPathsPerPage() throws IOException {
    List<String> paths = new java.util.ArrayList<>();
    for (int i = 0; i < 50; i++) {
      paths.add(String.format("s3://bucket/data/%07d-tiny.parquet", i));
    }
    Map<String, String> properties = new java.util.HashMap<>(writeProperties());
    properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1000000000");
    DeleteFile deleteFile = writeDeleteFile(properties, paths, 2);
    InputFile inputFile = Files.localInput(deleteFile.location());

    CharSequenceMap<PositionDeleteIndex> result =
        ParallelPositionDeleteCacheLoader.load(inputFile, deleteFile, 4);
    assertMatches(result, reference(deleteFile));
  }

  @Test
  void singlePath() throws IOException {
    List<String> paths = List.of("s3://bucket/data/0000000-only.parquet");
    DeleteFile deleteFile = writeDeleteFile(writeProperties(), paths, 20);
    InputFile inputFile = Files.localInput(deleteFile.location());

    CharSequenceMap<PositionDeleteIndex> result =
        ParallelPositionDeleteCacheLoader.load(inputFile, deleteFile, 4);
    assertMatches(result, reference(deleteFile));
  }
}
