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
import java.util.concurrent.TimeUnit;
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
 * Timing for {@link ParallelPositionDeleteCacheLoader}, the production implementation (in this
 * module's {@code src/main}) of the full-file cached load - see its javadoc for how it works.
 * Verified for correctness via {@code TestParallelPositionDeleteCacheLoader} in this module's
 * {@code src/test}; this class exists purely to measure real timing.
 *
 * <p>Split into its own class because it needs the full {@code rowGroupSizeMb} sweep: unlike the
 * sequential cached benchmarks (essentially flat across row-group count - see {@link
 * LoadPositionDeletesCachedBenchmark}'s javadoc), this one's whole point is row-group-count-driven
 * parallelism, and JMH applies one {@code @Param} set per class to every {@code @Benchmark} method
 * in it.
 *
 * <p>See {@link AbstractLoadPositionDeletesBenchmark} for the shared setup this class relies on.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-parquet:jmh
 *       -PjmhIncludeRegex=LoadPositionDeletesCachedParallelBenchmark
 *       -PjmhOutputPath=benchmark/load-position-deletes-cached-parallel-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
public class LoadPositionDeletesCachedParallelBenchmark
    extends AbstractLoadPositionDeletesBenchmark {

  @Param({"4", "128"})
  private int numDataFiles;

  @Param({"4", "8", "16", "32", "64"})
  private int rowGroupSizeMb;

  @Param("8")
  private int rowGroupThreadPoolSize;

  @Override
  protected int numDataFiles() {
    return numDataFiles;
  }

  @Override
  protected int rowGroupSizeMb() {
    return rowGroupSizeMb;
  }

  @Benchmark
  public void coldCacheDecodeDictionaryIdParallel(Blackhole blackhole) throws IOException {
    blackhole.consume(
        ParallelPositionDeleteCacheLoader.load(
            loadInputFile(deleteFile()), deleteFile(), rowGroupThreadPoolSize));
  }
}
