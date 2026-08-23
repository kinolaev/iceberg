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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.CharSequenceMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Cached-path variants of the {@code BaseDeleteLoader} caching decision from <a
 * href="https://github.com/apache/iceberg/issues/11648">#11648</a>: the cost of a cache-miss
 * lookup, which decodes and indexes an entire delete file once. Paid exactly once per delete file
 * by a caching loader, no matter how many distinct data files are looked up afterwards.
 *
 * <p>Lives in {@code org.apache.iceberg.deletes} (module {@code iceberg-parquet}, jmh source
 * set), not alongside {@code BaseDeleteLoader} in {@code org.apache.iceberg.data}: {@link
 * #coldCacheDecodeMain} needs package-private access to {@link BitmapPositionDeleteIndex} to build
 * a real one directly, instead of a hand-rolled proxy. {@code iceberg-parquet} already depends on
 * {@code iceberg-core}, so this is a legal (if unusual) classic-classpath package split - it would
 * break under JPMS strong encapsulation, which this project does not use. {@link
 * ParallelPositionDeleteReader} in this module's own {@code src/main} relies on the same split.
 *
 * <p>A full-file decode reads every row regardless of {@code numDataFiles} - total rows decoded is
 * exactly {@code numDeletes} - and neither benchmark here decodes row groups in parallel, so both
 * are also essentially flat across {@code rowGroupSizeMb}. This class only sweeps the {@code
 * numDataFiles} extremes (4, 128) and {@code rowGroupSizeMb} extremes (4, 64) to prove that
 * invariance empirically rather than sweep the full grid for no reason. See {@link
 * LoadPositionDeletesCachedParallelBenchmark} for a variant that does need the full {@code
 * rowGroupSizeMb} sweep, since row-group count is what its parallelism works with.
 *
 * <p>Combine {@link #coldCacheDecode} here with {@link
 * LoadPositionDeletesUncachedBenchmark#uncachedLookup} there: {@code N* = coldCacheDecode /
 * uncachedLookup} is the break-even number of same-file lookups above which caching wins.
 *
 * <p>See {@link AbstractLoadPositionDeletesBenchmark} for the shared setup this class relies on.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-parquet:jmh
 *       -PjmhIncludeRegex=LoadPositionDeletesCachedBenchmark
 *       -PjmhOutputPath=benchmark/load-position-deletes-cached-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
public class LoadPositionDeletesCachedBenchmark extends AbstractLoadPositionDeletesBenchmark {

  @Param({"4", "128"})
  private int numDataFiles;

  @Param({"4", "64"})
  private int rowGroupSizeMb;

  @Override
  protected int numDataFiles() {
    return numDataFiles;
  }

  @Override
  protected int rowGroupSizeMb() {
    return rowGroupSizeMb;
  }

  @Benchmark
  public void coldCacheDecode(Blackhole blackhole) {
    CloseableIterable<Record> deletes = openDeletes(deleteFile(), null);
    blackhole.consume(Deletes.toPositionIndexes(deletes, deleteFile()));
  }

  // same as coldCacheDecode, but via toPositionIndexes below - the pre-#15714 behavior that
  // re-hashes the map key every row instead of only on a path change - to quantify what the
  // rehashing fix bought.
  @Benchmark
  public void coldCacheDecodeMain(Blackhole blackhole) {
    CloseableIterable<Record> deletes = openDeletes(deleteFile(), null);
    blackhole.consume(toPositionIndexes(deletes, deleteFile()));
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
}
