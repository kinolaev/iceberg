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

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
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
 * The uncached-path half of the {@code BaseDeleteLoader} caching decision from <a
 * href="https://github.com/apache/iceberg/issues/11648">#11648</a>: the cost of a single filtered,
 * non-caching lookup against the real production path.
 *
 * <p>Unlike the cached path (see {@link LoadPositionDeletesCachedBenchmark}), a lookup's cost
 * genuinely depends on both {@code numDataFiles} (how many row groups the dictionary skip check can
 * rule out) and {@code rowGroupSizeMb} (how many row groups exist to check), so this class sweeps
 * the full grid of both.
 *
 * <p>Combine {@link #uncachedLookup} here with {@link
 * LoadPositionDeletesCachedBenchmark#coldCacheDecode} there: {@code N* = coldCacheDecode /
 * uncachedLookup} is the break-even point.
 *
 * <p>See {@link AbstractLoadPositionDeletesBenchmark} for the shared setup this class relies on.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-parquet:jmh
 *       -PjmhIncludeRegex=LoadPositionDeletesUncachedBenchmark
 *       -PjmhOutputPath=benchmark/load-position-deletes-uncached-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
public class LoadPositionDeletesUncachedBenchmark extends AbstractLoadPositionDeletesBenchmark {

  @Param({"4", "8", "16", "32", "64", "128"})
  private int numDataFiles;

  @Param({"4", "8", "16", "32", "64"})
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
  public void uncachedLookup(Blackhole blackhole) {
    String filePath = nextPath();
    Expression filter = Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), filePath);
    CloseableIterable<org.apache.iceberg.data.Record> deletes = openDeletes(deleteFile(), filter);
    blackhole.consume(Deletes.toPositionIndex(filePath, deletes, deleteFile()));
  }
}
