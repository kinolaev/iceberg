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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParallelPositionDeleteReader {

  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();

  @TempDir private File temp;

  // tiny row-group/page sizes so a handful of paths already spans multiple row groups and
  // multiple pages per row group, exercising SOLO/FIRST/MIDDLE/LAST without needing huge data
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

  private DeleteFile writeDeleteFile(List<String> paths, int positionsPerPath) throws IOException {
    File file = new File(temp, "deletes-" + System.nanoTime() + ".parquet");
    OutputFile out = Files.localOutput(file);
    PositionDeleteWriter<Void> writer =
        Parquet.writeDeletes(out)
            .withSpec(PartitionSpec.unpartitioned())
            .setAll(writeProperties())
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

  // ground truth: reads every record in the file and lets Deletes.toPositionIndex do its own
  // (already-validated, production) path filtering - no dependency on the code under test
  private PositionDeleteIndex reference(DeleteFile deleteFile, String targetPath)
      throws IOException {
    try (CloseableIterable<Record> deletes =
        Parquet.read(Files.localInput(deleteFile.location()))
            .project(POS_DELETE_SCHEMA)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(POS_DELETE_SCHEMA, fileSchema))
            .reuseContainers()
            .build()) {
      return Deletes.toPositionIndex(targetPath, deletes, deleteFile);
    }
  }

  private void assertMatches(
      PositionDeleteIndex actual, PositionDeleteIndex expected, String path) {
    assertThat(actual.cardinality())
        .as("cardinality for %s", path)
        .isEqualTo(expected.cardinality());
    List<Long> missing = Lists.newArrayList();
    expected.forEach(
        pos -> {
          if (!actual.isDeleted(pos)) {
            missing.add(pos);
          }
        });
    assertThat(missing).as("positions missing for %s", path).isEmpty();
  }

  @Test
  void soloFirstMiddleLastRoles() throws IOException {
    List<String> paths =
        List.of(
            "s3://bucket/data/0000000-a.parquet",
            "s3://bucket/data/0000001-b.parquet",
            "s3://bucket/data/0000002-c.parquet",
            "s3://bucket/data/0000003-d.parquet",
            "s3://bucket/data/0000004-e.parquet",
            "s3://bucket/data/0000005-f.parquet");
    DeleteFile deleteFile = writeDeleteFile(paths, 20);
    InputFile inputFile = Files.localInput(deleteFile.location());

    for (String path : paths) {
      PositionDeleteIndex result =
          ParallelPositionDeleteReader.read(inputFile, deleteFile, path, 4);
      assertMatches(result, reference(deleteFile, path), path);
    }
  }

  // one big row group so every path's match is a single-row-group SOLO case, exercising both the
  // same-page and different-boundary-page sub-cases
  private DeleteFile writeSingleRowGroupDeleteFile(List<String> paths, int positionsPerPath)
      throws IOException {
    File file = new File(temp, "deletes-solo-" + System.nanoTime() + ".parquet");
    OutputFile out = Files.localOutput(file);
    Map<String, String> properties = new java.util.HashMap<>(writeProperties());
    properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1000000000");
    PositionDeleteWriter<Void> writer =
        Parquet.writeDeletes(out)
            .withSpec(PartitionSpec.unpartitioned())
            .setAll(properties)
            .overwrite()
            .buildPositionWriter();
    PositionDelete<Void> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<Void> closeableWriter = writer) {
      for (String path : paths) {
        for (int pos = 0; pos < positionsPerPath; pos++) {
          closeableWriter.write(posDelete.set(path, pos * 2L));
        }
      }
    }
    return writer.toDeleteFile();
  }

  @Test
  void singleRowGroupSolo() throws IOException {
    List<String> paths =
        List.of(
            "s3://bucket/data/0000000-a.parquet",
            "s3://bucket/data/0000001-b.parquet",
            "s3://bucket/data/0000002-c.parquet");
    DeleteFile deleteFile = writeSingleRowGroupDeleteFile(paths, 30);
    InputFile inputFile = Files.localInput(deleteFile.location());

    for (String path : paths) {
      PositionDeleteIndex result =
          ParallelPositionDeleteReader.read(inputFile, deleteFile, path, 4);
      assertMatches(result, reference(deleteFile, path), path);
    }
  }

  @Test
  void countsSeeksForSoloMatchWithFarApartBoundaryPages() throws IOException {
    // enough distinct paths and positions that the middle path's own data spans many pages
    // within the single row group, so firstCandidate != lastCandidate (matches the 128-file/
    // 64MB-row-group JMH scenario's shape)
    List<String> paths = new java.util.ArrayList<>();
    for (int i = 0; i < 20; i++) {
      paths.add(String.format("s3://bucket/data/%07d-data.parquet", i));
    }
    DeleteFile deleteFile = writeSingleRowGroupDeleteFile(paths, 200);
    String targetPath = paths.get(10);

    SeekCountingInputFile counting =
        new SeekCountingInputFile(Files.localInput(deleteFile.location()));
    PositionDeleteIndex result =
        ParallelPositionDeleteReader.read(counting, deleteFile, targetPath, 4);
    assertMatches(result, reference(deleteFile, targetPath), targetPath);
    System.out.println("[seekcount] seeks=" + counting.seekCount.get());
  }

  // pure test-side instrumentation - no production code changes. A seek is a reasonable proxy
  // for a distinct underlying range request: sequential read() calls without an intervening seek
  // continue an already-open connection/position
  private static final class SeekCountingInputFile implements InputFile {
    private final InputFile delegate;
    final java.util.concurrent.atomic.AtomicInteger seekCount =
        new java.util.concurrent.atomic.AtomicInteger();

    SeekCountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }

    @Override
    public org.apache.iceberg.io.SeekableInputStream newStream() {
      org.apache.iceberg.io.SeekableInputStream in = delegate.newStream();
      return new org.apache.iceberg.io.SeekableInputStream() {
        @Override
        public long getPos() throws IOException {
          return in.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
          seekCount.incrementAndGet();
          in.seek(newPos);
        }

        @Override
        public int read() throws IOException {
          return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return in.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
          in.close();
        }
      };
    }
  }

  @Test
  void pathNotPresent() throws IOException {
    List<String> paths =
        List.of("s3://bucket/data/0000000-a.parquet", "s3://bucket/data/0000001-b.parquet");
    DeleteFile deleteFile = writeDeleteFile(paths, 20);
    InputFile inputFile = Files.localInput(deleteFile.location());

    PositionDeleteIndex result =
        ParallelPositionDeleteReader.read(
            inputFile, deleteFile, "s3://bucket/data/does-not-exist.parquet", 4);
    assertThat(result.cardinality()).isEqualTo(0);
  }

  @Test
  void singlePath() throws IOException {
    List<String> paths = List.of("s3://bucket/data/0000000-only.parquet");
    DeleteFile deleteFile = writeDeleteFile(paths, 20);
    InputFile inputFile = Files.localInput(deleteFile.location());

    PositionDeleteIndex result =
        ParallelPositionDeleteReader.read(inputFile, deleteFile, paths.get(0), 4);
    assertMatches(result, reference(deleteFile, paths.get(0)), paths.get(0));
  }
}
