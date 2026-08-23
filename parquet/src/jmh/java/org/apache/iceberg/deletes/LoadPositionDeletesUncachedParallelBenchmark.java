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
 * Timing for {@link ParallelPositionDeleteReader}, the production implementation (in this
 * module's {@code src/main}) of the fully-optimized uncached lookup algorithm - see its javadoc
 * for how it works. Verified for correctness via {@code TestParallelPositionDeleteReader} in this
 * module's {@code src/test}; this class exists purely to measure real timing against {@link
 * LoadPositionDeletesUncachedBenchmark#uncachedLookup}.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-parquet:jmh
 *       -PjmhIncludeRegex=LoadPositionDeletesUncachedParallelBenchmark
 *       -PjmhOutputPath=benchmark/load-position-deletes-uncached-parallel-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
public class LoadPositionDeletesUncachedParallelBenchmark
    extends AbstractLoadPositionDeletesBenchmark {

  @Param({"4", "8", "16", "32", "64", "128"})
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
  public void dictionaryIdFilteredReadParallel(Blackhole blackhole) throws IOException {
    blackhole.consume(
        ParallelPositionDeleteReader.read(
            loadInputFile(deleteFile()), deleteFile(), nextPath(), rowGroupThreadPoolSize));
  }
}
