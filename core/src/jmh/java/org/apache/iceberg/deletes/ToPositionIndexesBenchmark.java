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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of {@link
 * Deletes#toPositionIndexes(CloseableIterable)}.
 *
 * <p>Position delete files are typically written sorted by (path, pos), meaning consecutive rows
 * usually share the same path. {@code pathSorted} benchmarks match that layout, while {@code
 * pathShuffled} benchmarks cover the worst case where the path changes on every row.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=ToPositionIndexesBenchmark
 *       -PjmhOutputPath=benchmark/to-position-indexes-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class ToPositionIndexesBenchmark {
  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();
  private static final Accessor<StructLike> FILENAME_ACCESSOR =
      POS_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_PATH.fieldId());
  private static final Accessor<StructLike> POSITION_ACCESSOR =
      POS_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_POS.fieldId());

  @Param("1000000")
  private int numDeletes;

  @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
  private int numDataFiles;

  private List<StructLike> deletes;
  private String pathTemplate =
      "s3://pretty-long-bucket-name/pretty-long-namespace-name/pretty-long-table-name/data/%07d-data.parquet";

  @Setup
  public void setupBenchmark() {
    deletes = Lists.newArrayListWithExpectedSize(numDeletes);
    int numDeletesPerDataFile = numDeletes / numDataFiles;
    for (int index = 0; index < numDeletes; index++) {
      String path = String.format(Locale.ROOT, pathTemplate, index / numDeletesPerDataFile);
      deletes.add(new PositionDeleteRow(path, index % numDeletesPerDataFile));
    }
  }

  @Benchmark
  @Threads(1)
  public void current(Blackhole blackhole) {
    blackhole.consume(toPositionIndexes(CloseableIterable.withNoopClose(deletes), null));
  }

  // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/deletes/Deletes.java#L139
  private static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> toPositionIndexes(
      CloseableIterable<T> posDeletes, DeleteFile file) {
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();

    try (CloseableIterable<T> deletes = posDeletes) {
      for (T delete : deletes) {
        CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
        long position = (long) POSITION_ACCESSOR.get(delete);
        PositionDeleteIndex index =
            indexes.computeIfAbsent(filePath, key -> new BitmapPositionDeleteIndex(file));
        index.delete(position);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close position delete source", e);
    }

    return indexes;
  }

  @Benchmark
  @Threads(1)
  public void pr15714(Blackhole blackhole) {
    blackhole.consume(Deletes.toPositionIndexes(CloseableIterable.withNoopClose(deletes)));
  }

  private record PositionDeleteRow(String path, long position) implements StructLike {
    @Override
    public int size() {
      return 2;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      switch (pos) {
        case 0:
          return (T) path;
        case 1:
          return (T) (Long) position;
        default:
          throw new UnsupportedOperationException("Unsupported position: " + pos);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}
