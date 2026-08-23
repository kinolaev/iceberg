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
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetInputFileAdapter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Shared infrastructure for {@link LoadPositionDeletesCachedBenchmark}, {@link
 * LoadPositionDeletesCachedParallelBenchmark}, {@link LoadPositionDeletesUncachedBenchmark}, and
 * {@link LoadPositionDeletesUncachedParallelBenchmark}, which together isolate the costs behind
 * the {@code BaseDeleteLoader} caching decision from <a
 * href="https://github.com/apache/iceberg/issues/11648">#11648</a> - both the real production path
 * and this module's own optimized {@link ParallelPositionDeleteReader}/{@link
 * ParallelPositionDeleteCacheLoader} - backed by a real object store (rustfs, an S3-compatible
 * server started via the repo's {@code compose.yaml}, expected on {@code localhost:9000}).
 *
 * <p>A single position delete file with {@code numDeletes} rows is written once per {@code
 * (numDataFiles, rowGroupSizeMb)} combination, sorted by path then position (mirroring real
 * position delete files) and spread across {@code numDataFiles} data file paths. Subclasses declare
 * their own {@code numDataFiles}/{@code rowGroupSizeMb} {@code @Param} sets (deliberately different
 * - see each subclass's javadoc) and implement {@link #numDataFiles()}/{@link #rowGroupSizeMb()} so
 * {@link #setupBenchmark()} here can call {@link #setupCommon(int, int)}.
 *
 * <p>The delete file's S3 key is fully determined by {@code (numDeletes, numDataFiles,
 * rowGroupSizeMb)}. JMH forks a separate trial per (benchmark method, param combination) pair, so
 * every method sharing a combo would otherwise rewrite an identical file; {@link #setupCommon(int,
 * int)} skips the rewrite if that key already exists. This assumes the write logic stays
 * deterministic for a given key - change one without the other and a stale file gets reused.
 */
@State(Scope.Benchmark)
public abstract class AbstractLoadPositionDeletesBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLoadPositionDeletesBenchmark.class);

  private static final String ENDPOINT = "http://localhost:9000";
  private static final String ACCESS_KEY_ID = "rustfs";
  private static final String SECRET_ACCESS_KEY = "rustfs";
  private static final String REGION = "us-east-1";
  private static final String BUCKET = "load-position-deletes-benchmark";

  private static final String PATH_TEMPLATE = "s3://" + BUCKET + "/table/data/%07d-data.parquet";

  protected static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();
  protected static final Accessor<StructLike> FILENAME_ACCESSOR =
      POS_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_PATH.fieldId());
  protected static final Accessor<StructLike> POSITION_ACCESSOR =
      POS_DELETE_SCHEMA.accessorForField(MetadataColumns.DELETE_FILE_POS.fieldId());

  @Param("67108864")
  private int numDeletes;

  private S3FileIO fileIO;
  private DeleteFile deleteFile;
  private List<String> paths;
  private int nextPathIndex;

  // subclasses' numDataFiles/rowGroupSizeMb are @Param fields with different value sets, so
  // setupBenchmark() can't read them as inherited fields - these accessors let it live here once
  protected abstract int numDataFiles();

  protected abstract int rowGroupSizeMb();

  @Setup
  public void setupBenchmark() throws IOException {
    setupCommon(numDataFiles(), rowGroupSizeMb());
  }

  protected void setupCommon(int numDataFiles, int rowGroupSizeMb) throws IOException {
    this.fileIO = newFileIO();
    createBucketIfNotExists();

    this.paths = Lists.newArrayListWithExpectedSize(numDataFiles);
    for (int file = 0; file < numDataFiles; file++) {
      paths.add(String.format(Locale.ROOT, PATH_TEMPLATE, file));
    }

    String location = deleteFileLocation(numDataFiles, rowGroupSizeMb);
    InputFile existing = fileIO.newInputFile(location);
    if (existing.exists()) {
      // dedup: reuse a previous trial's file for this combo instead of rewriting it (see class
      // javadoc)
      this.deleteFile =
          FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
              .ofPositionDeletes()
              .withPath(location)
              .withFormat(FileFormat.PARQUET)
              .withFileSizeInBytes(existing.getLength())
              .withRecordCount(numDeletes)
              .build();
    } else {
      this.deleteFile =
          writeDeleteFile(fileIO.newOutputFile(location), paths, numDataFiles, rowGroupSizeMb);
    }

    logRowGroupStats(deleteFile, numDataFiles);
  }

  @TearDown
  public void tearDownCommon() {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  private String deleteFileLocation(int numDataFiles, int rowGroupSizeMb) {
    return String.format(
        Locale.ROOT,
        "s3://%s/table/data/%d-deletes-%d-files-%dmb-rowgroups.parquet",
        BUCKET,
        numDeletes,
        numDataFiles,
        rowGroupSizeMb);
  }

  // matches the write.parquet.* defaults a real table would use, except row-group-size-bytes
  // (swept via rowGroupSizeMb instead of the 128 MB default): with the default
  // write.delete.target-file-size-bytes of 64 MB, RollingFileWriter force-flushes every real
  // delete file's row group well before 128 MB, so 64 MB - not 128 MB - is what a prod delete
  // file's row group actually ends up being
  private Map<String, String> writeProperties(int rowGroupSizeMb) {
    return ImmutableMap.<String, String>builder()
        .put(
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            Long.toString((long) rowGroupSizeMb * 1024 * 1024))
        .put(
            TableProperties.PARQUET_COMPRESSION,
            TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0)
        .put(
            TableProperties.PARQUET_PAGE_SIZE_BYTES,
            Integer.toString(TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT))
        .put(
            TableProperties.PARQUET_PAGE_ROW_LIMIT,
            Integer.toString(TableProperties.PARQUET_PAGE_ROW_LIMIT_DEFAULT))
        .put(
            TableProperties.PARQUET_DICT_SIZE_BYTES,
            Integer.toString(TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT))
        .put(TableProperties.PARQUET_PAGE_VERSION, TableProperties.PARQUET_PAGE_VERSION_DEFAULT)
        .put(
            TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
            Integer.toString(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT))
        .put(
            TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
            Integer.toString(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT))
        .put(
            TableProperties.PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED,
            Boolean.toString(TableProperties.PARQUET_ROW_GROUP_SIZE_TRACK_UNCOMPRESSED_DEFAULT))
        // raises parquet's 64-byte column-index truncate-length default: file_path values share
        // a long common prefix (bucket + warehouse path + namespace + table), so without this
        // every page's truncated min/max collapses to the same value and page pruning never
        // fires. Not an Iceberg TableProperties key - set via the raw
        // parquet.columnindex.truncate.length config key.
        .put("parquet.columnindex.truncate.length", "256")
        // write.parquet.compression-level is intentionally omitted: its default is null, meaning
        // "let the codec pick its own default level"
        .build();
  }

  // writes deletes as contiguous per-path blocks, i.e. sorted by (path, pos) as position delete
  // files typically are
  private DeleteFile writeDeleteFile(
      OutputFile out, List<String> dataFilePaths, int numDataFiles, int rowGroupSizeMb)
      throws IOException {
    int numDeletesPerDataFile = numDeletes / numDataFiles;

    PositionDeleteWriter<Void> writer =
        Parquet.writeDeletes(out)
            .withSpec(PartitionSpec.unpartitioned())
            .setAll(writeProperties(rowGroupSizeMb))
            .overwrite()
            .buildPositionWriter();

    PositionDelete<Void> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<Void> closeableWriter = writer) {
      for (int i = 0; i < dataFilePaths.size(); i++) {
        String path = dataFilePaths.get(i);
        long shift = (long) i * numDeletesPerDataFile;
        for (int pos = 0; pos < numDeletesPerDataFile; pos++) {
          closeableWriter.write(posDelete.set(path, (shift + pos) * 2));
        }
      }
    }

    return writer.toDeleteFile();
  }

  // diagnostic: prints row-group boundaries and file_path min/max, to check whether row-group
  // pruning can actually skip anything at this numDataFiles selectivity
  private void logRowGroupStats(DeleteFile file, int numDataFiles) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetInputFileAdapter.of(loadInputFile(file)))) {
      ParquetMetadata footer = reader.getFooter();
      List<BlockMetaData> blocks = footer.getBlocks();
      LOG.info(
          "[row-group stats] numDataFiles={} file={} rowGroups={}",
          numDataFiles,
          file.location(),
          blocks.size());
      for (int i = 0; i < blocks.size(); i++) {
        BlockMetaData block = blocks.get(i);
        ColumnChunkMetaData pathColumn = findPathColumn(block);
        Statistics<?> stats = pathColumn == null ? null : pathColumn.getStatistics();
        String min = stats == null || stats.isEmpty() ? "?" : stats.minAsString();
        String max = stats == null || stats.isEmpty() ? "?" : stats.maxAsString();
        ColumnIndexDiagnostics diagnostics = describeColumnIndexes(reader, pathColumn);

        LOG.info(
            "[row-group stats]   rowGroup={} rows={} file_path min={} max={}"
                + " columnIndex={} offsetIndex={}",
            i,
            block.getRowCount(),
            min,
            max,
            diagnostics.columnIndexInfo(),
            diagnostics.offsetIndexInfo());
      }
    }
  }

  private static ColumnChunkMetaData findPathColumn(BlockMetaData block) {
    for (ColumnChunkMetaData column : block.getColumns()) {
      if (column.getPath().toDotString().equals("file_path")) {
        return column;
      }
    }
    return null;
  }

  private record ColumnIndexDiagnostics(String columnIndexInfo, String offsetIndexInfo) {}

  private static ColumnIndexDiagnostics describeColumnIndexes(
      ParquetFileReader reader, ColumnChunkMetaData pathColumn) {
    if (pathColumn == null) {
      return new ColumnIndexDiagnostics("n/a", "n/a");
    }

    String columnIndexInfo;
    try {
      org.apache.parquet.internal.column.columnindex.ColumnIndex columnIndex =
          reader.readColumnIndex(pathColumn);
      columnIndexInfo =
          columnIndex == null ? "NULL (not written)" : describeColumnIndex(columnIndex);
    } catch (Exception e) {
      columnIndexInfo = "ERROR: " + e;
    }

    String offsetIndexInfo;
    try {
      org.apache.parquet.internal.column.columnindex.OffsetIndex offsetIndex =
          reader.readOffsetIndex(pathColumn);
      offsetIndexInfo =
          offsetIndex == null
              ? "NULL (not written)"
              : "present, pages=" + offsetIndex.getPageCount();
    } catch (Exception e) {
      offsetIndexInfo = "ERROR: " + e;
    }

    return new ColumnIndexDiagnostics(columnIndexInfo, offsetIndexInfo);
  }

  private static String describeColumnIndex(
      org.apache.parquet.internal.column.columnindex.ColumnIndex columnIndex) {
    int pageCount = columnIndex.getMinValues().size();
    return String.format(
        Locale.ROOT,
        "present, pages=%d minOfFirstPage=%s maxOfFirstPage=%s",
        pageCount,
        pageCount > 0
            ? Binary.fromConstantByteBuffer(columnIndex.getMinValues().get(0)).toStringUsingUTF8()
            : "?",
        pageCount > 0
            ? Binary.fromConstantByteBuffer(columnIndex.getMaxValues().get(0)).toStringUsingUTF8()
            : "?");
  }

  // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java#L220
  protected CloseableIterable<org.apache.iceberg.data.Record> openDeletes(
      DeleteFile file, Expression filter) {
    ReadBuilder<org.apache.iceberg.data.Record, ?> builder =
        FormatModelRegistry.readBuilder(file.format(), Record.class, loadInputFile(file));
    return builder.project(POS_DELETE_SCHEMA).reuseContainers().filter(filter).build();
  }

  // rotates through every path so measurements aren't biased toward whichever path sits first
  // (e.g. its row group or map slot)
  protected String nextPath() {
    String path = paths.get(nextPathIndex);
    nextPathIndex = (nextPathIndex + 1) % paths.size();
    return path;
  }

  protected InputFile loadInputFile(DeleteFile file) {
    return fileIO.newInputFile(file.location(), file.fileSizeInBytes());
  }

  protected DeleteFile deleteFile() {
    return deleteFile;
  }

  private static S3FileIO newFileIO() {
    S3FileIO fileIO = new S3FileIO();
    fileIO.initialize(
        ImmutableMap.of(
            S3FileIOProperties.ENDPOINT, ENDPOINT,
            S3FileIOProperties.PATH_STYLE_ACCESS, "true",
            S3FileIOProperties.ACCESS_KEY_ID, ACCESS_KEY_ID,
            S3FileIOProperties.SECRET_ACCESS_KEY, SECRET_ACCESS_KEY,
            AwsClientProperties.CLIENT_REGION, REGION));
    return fileIO;
  }

  private static void createBucketIfNotExists() {
    S3Client client =
        S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY)))
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.of(REGION))
            .forcePathStyle(true)
            .build();
    try (S3Client closeableClient = client) {
      closeableClient.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
      // bucket already exists from a previous run, nothing to do
    }
  }
}
