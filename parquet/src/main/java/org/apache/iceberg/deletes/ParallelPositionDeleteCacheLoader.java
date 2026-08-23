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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/**
 * Loads every path's position deletes out of a position delete file in one pass, exploiting the
 * spec-guaranteed sort order (by path then position) the same way {@link
 * ParallelPositionDeleteReader} does for a single target path - except here the goal is to cover
 * every distinct path in the file, so instead of narrowing down to one or two boundary pages, this
 * classifies every page in {@code file_path}'s column index up front:
 *
 * <ul>
 *   <li><b>homogeneous</b> page (its column index min equals its max): the whole page is guaranteed
 *       to be one path, already known directly off the column index.
 *   <li><b>mixed</b> page (min != max): contains at least one path transition, so it needs an
 *       actual per-row dictionary-id decode to find where.
 * </ul>
 *
 * <p>{@code file_path} is read via a single {@code readFilteredRowGroup} scoped to just the mixed
 * pages (one combined read, not one per page); homogeneous pages are never fetched for {@code
 * file_path} at all, since their one path is already known from the column index. This matters
 * because Parquet decompresses a column chunk's pages lazily, one at a time, as the reader advances
 * past each page boundary - so even calling a non-materializing method like {@code
 * ColumnReader.skip()} instead of {@code getCurrentValueDictionaryID()} still forces every page it
 * passes through to be decompressed. The only way to actually skip that cost is to never fetch the
 * page at all. Doing so is safe here - unlike the fragmented-many-small-reads problem {@link
 * ParallelPositionDeleteReader} has to reason about for its single-target boundary-page reads -
 * because by the time any row group is decoded, the whole file already sits in memory (see below):
 * scoping a read to scattered pages costs nothing extra once "fetching" one just means reading from
 * an already-local buffer. {@code pos} still needs its own unfiltered pass regardless - every row's
 * position is needed, no matter which page it's in.
 *
 * <p>Since data is sorted by {@code (path, pos)}, a path's rows can only ever be split across two
 * places: multiple groups within one row group are always for different paths, and the same path
 * can only reappear in an adjacent row group (never a non-adjacent one) - both are handled the same
 * way {@link ParallelPositionDeleteReader} handles cross-row-group continuation: each row group
 * runs as its own task, and same-key results from different tasks are merged afterward.
 *
 * <p>Unlike {@link ParallelPositionDeleteReader}, which deliberately avoids reading most of the
 * file for a single target path, this loader's whole point is to read the entire file - so instead
 * of each row-group task independently fetching its own bytes over the network (N row groups means
 * N separate requests, run concurrently across the thread pool), the whole file is fetched once, up
 * front, into memory via {@link org.apache.iceberg.io.IOUtil#readFully}, which uses {@code
 * RangeReadable.readFully} directly when available - a single bounded request instead of per-row-
 * group seeks. Every row-group task then reads from that shared, read-only, in-memory buffer
 * instead of the network, via a trivial in-memory {@code InputFile}/{@code SeekableInputStream} -
 * each task's stream has its own independent position over the same backing bytes, safe to read
 * concurrently. This turns row-group parallelism into pure CPU-bound decoding, no longer coupled to
 * how many separate network round trips the row-group count happens to imply.
 */
public class ParallelPositionDeleteCacheLoader {

  private ParallelPositionDeleteCacheLoader() {}

  @SuppressWarnings("CollectionUndefinedEquality")
  public static CharSequenceMap<PositionDeleteIndex> load(
      org.apache.iceberg.io.InputFile icebergInputFile,
      DeleteFile deleteFile,
      int rowGroupThreadPoolSize)
      throws IOException {
    InputFile inputFile = inMemoryInputFile(fetchWholeFile(icebergInputFile));
    ParquetMetadata footer = ParallelPositionDeleteReader.readFooter(inputFile);
    MessageType schema = footer.getFileMetaData().getSchema();
    String createdBy = footer.getFileMetaData().getCreatedBy();
    ColumnDescriptor pathDescriptor = schema.getColumnDescription(new String[] {"file_path"});
    ColumnDescriptor posDescriptor = schema.getColumnDescription(new String[] {"pos"});
    List<BlockMetaData> blocks = footer.getBlocks();

    ReadContext context =
        new ReadContext(
            inputFile, footer, schema, createdBy, pathDescriptor, posDescriptor, deleteFile);

    if (blocks.size() == 1) {
      // nothing to parallelize - skip the executor/merge machinery entirely
      return decodeRowGroup(context, 0);
    }

    ExecutorService executor =
        Executors.newFixedThreadPool(Math.min(rowGroupThreadPoolSize, blocks.size()));
    try {
      CompletionService<CharSequenceMap<PositionDeleteIndex>> completionService =
          new ExecutorCompletionService<>(executor);
      for (int i = 0; i < blocks.size(); i++) {
        int rowGroupIndex = i;
        completionService.submit(() -> decodeRowGroup(context, rowGroupIndex));
      }

      // a path's deletes can straddle a row-group boundary (but only an adjacent one, by sort
      // order), so merge same-key results instead of assuming every task's keys are disjoint -
      // merge the smaller index into the bigger one in place instead of allocating a third bitmap
      CharSequenceMap<PositionDeleteIndex> merged = CharSequenceMap.create();
      for (int i = 0; i < blocks.size(); i++) {
        CharSequenceMap<PositionDeleteIndex> partial = completionService.take().get();
        for (Map.Entry<CharSequence, PositionDeleteIndex> entry : partial.entrySet()) {
          PositionDeleteIndex existing = merged.get(entry.getKey());
          PositionDeleteIndex incoming = entry.getValue();
          if (existing == null) {
            merged.put(entry.getKey(), incoming);
          } else if (existing.cardinality() >= incoming.cardinality()) {
            existing.merge(incoming);
          } else {
            incoming.merge(existing);
            merged.put(entry.getKey(), incoming);
          }
        }
      }
      return merged;
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to load position deletes", e);
    } finally {
      executor.shutdown();
    }
  }

  // bundles the arguments every row-group task shares (constant across a single load() call)
  private record ReadContext(
      InputFile inputFile,
      ParquetMetadata footer,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      ColumnDescriptor posDescriptor,
      DeleteFile deleteFile) {}

  // one contiguous run of rows within a row group belonging to a single path - lastRow is
  // inclusive; a group's own first row is implicit (the previous group's lastRow + 1, or 0 for the
  // first group in the row group)
  private record PathGroup(String path, long lastRow) {}

  // file_path's column index, materialized once per row group (see the O(pageCount) comment where
  // this is built) and passed around as a unit instead of four separate parameters
  private record PathColumnIndex(
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      OffsetIndex offsetIndex,
      int pageCount) {}

  private static CharSequenceMap<PositionDeleteIndex> decodeRowGroup(
      ReadContext context, int rowGroupIndex) {
    InputFile inputFile = context.inputFile();
    ParquetMetadata footer = context.footer();
    MessageType schema = context.schema();
    String createdBy = context.createdBy();
    ColumnDescriptor pathDescriptor = context.pathDescriptor();
    ColumnDescriptor posDescriptor = context.posDescriptor();
    DeleteFile deleteFile = context.deleteFile();
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            inputFile, footer, ParquetReadOptions.builder().build(), inputFile.newStream())) {
      BlockMetaData block = footer.getBlocks().get(rowGroupIndex);
      long blockRowCount = block.getRowCount();

      ColumnChunkMetaData pathColumn = null;
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getPath().toDotString().equals("file_path")) {
          pathColumn = column;
          break;
        }
      }

      // prime the reader's ColumnIndexStore cache for this row group with both columns before
      // any restricted setRequestedSchema call - see ParallelPositionDeleteReader.decodeRowGroup
      // for why this ordering matters
      reader.setRequestedSchema(Arrays.asList(pathDescriptor, posDescriptor));
      ColumnIndexStore ciStore = reader.getColumnIndexStore(rowGroupIndex);
      ColumnIndex columnIndex = ciStore.getColumnIndex(pathColumn.getPath());
      OffsetIndex offsetIndex = ciStore.getOffsetIndex(pathColumn.getPath());
      // getMinValues()/getMaxValues() rebuild their entire list - decoding every page's value - on
      // every call (see ColumnIndexBuilder.ColumnIndexBase); getNullPages() is a cheap array wrap,
      // used here just for the count. Both lists are fetched once and reused below instead of
      // rebuilt per page - O(pageCount) instead of O(pageCount^2) across the loops below
      PathColumnIndex pathColumnIndex =
          new PathColumnIndex(
              columnIndex.getMinValues(),
              columnIndex.getMaxValues(),
              offsetIndex,
              columnIndex.getNullPages().size());

      List<PathGroup> groups =
          buildPathGroups(
              reader,
              schema,
              createdBy,
              pathDescriptor,
              block,
              rowGroupIndex,
              pathColumnIndex,
              blockRowCount);

      return readPosGroupedByPath(
          reader,
          schema,
          createdBy,
          posDescriptor,
          rowGroupIndex,
          groups,
          blockRowCount,
          deleteFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // classifies every page as homogeneous (min == max: the whole page is one path, readable
  // directly off the column index, never fetched for file_path at all) or mixed (needs an actual
  // row scan), then walks pages in order to build the list of (path, lastRow) groups covering the
  // row group. All mixed pages are fetched in a single combined filtered read - not one read per
  // mixed page - so file_path's dictionary page (needed even for ID-only reads, since Parquet's
  // ColumnReaderBase always requires a non-null Dictionary to build its value reader) is fetched
  // at most once per row group, no matter how many mixed pages it has
  private static List<PathGroup> buildPathGroups(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      BlockMetaData block,
      int rowGroupIndex,
      PathColumnIndex pathColumnIndex,
      long blockRowCount)
      throws IOException {
    List<ByteBuffer> minValues = pathColumnIndex.minValues();
    List<ByteBuffer> maxValues = pathColumnIndex.maxValues();
    OffsetIndex offsetIndex = pathColumnIndex.offsetIndex();
    int pageCount = pathColumnIndex.pageCount();

    List<Integer> mixedPages = findMixedPages(minValues, maxValues, pageCount);
    MixedPageReader mixedReader =
        openMixedPageReader(
            reader,
            schema,
            createdBy,
            pathDescriptor,
            block,
            rowGroupIndex,
            mixedPages,
            offsetIndex,
            blockRowCount);

    GroupAccumulator accumulator = new GroupAccumulator();
    for (int page = 0; page < pageCount; page++) {
      long firstRow = offsetIndex.getFirstRowIndex(page);
      long lastRow = offsetIndex.getLastRowIndex(page, blockRowCount);
      if (minValues.get(page).compareTo(maxValues.get(page)) == 0) {
        // homogeneous - value known directly from the column index, no fetch or decode needed
        accumulator.extend(Binary.fromConstantByteBuffer(minValues.get(page)), lastRow);
      } else {
        // mixed - consume this page's rows from the shared mixed-pages-only reader
        consumeMixedPage(mixedReader, firstRow, lastRow, accumulator);
      }
    }
    return accumulator.finish();
  }

  private static List<Integer> findMixedPages(
      List<ByteBuffer> minValues, List<ByteBuffer> maxValues, int pageCount) {
    List<Integer> mixedPages = Lists.newArrayList();
    for (int p = 0; p < pageCount; p++) {
      if (minValues.get(p).compareTo(maxValues.get(p)) != 0) {
        mixedPages.add(p);
      }
    }
    return mixedPages;
  }

  // a single file_path-only reader (plus its dictionary) covering every mixed page in the row
  // group, opened via one combined filtered read - not one per mixed page, so the dictionary page
  // is fetched at most once per row group no matter how many mixed pages it has. Null if the row
  // group has no mixed pages at all
  private record MixedPageReader(ColumnReader pathReader, Dictionary dictionary) {}

  private static MixedPageReader openMixedPageReader(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      BlockMetaData block,
      int rowGroupIndex,
      List<Integer> mixedPages,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    if (mixedPages.isEmpty()) {
      return null;
    }

    DictionaryPageReadStore dictionaryReader = reader.getDictionaryReader(block);
    DictionaryPage dictionaryPage = dictionaryReader.readDictionaryPage(pathDescriptor);
    Dictionary dictionary = dictionaryPage.getEncoding().initDictionary(pathDescriptor, dictionaryPage);

    RowRanges mixedRange =
        RowRanges.create(
            blockRowCount, mixedPages.stream().mapToInt(Integer::intValue).iterator(), offsetIndex);
    PageReadStore pageReadStore =
        ParallelPositionDeleteReader.openPathOnlyRange(
            reader, pathDescriptor, rowGroupIndex, mixedRange);
    ColumnReader pathReader =
        ParallelPositionDeleteReader.pathOnlyColumnReader(
            pageReadStore, schema, createdBy, pathDescriptor);
    return new MixedPageReader(pathReader, dictionary);
  }

  // decoding a new value only when the dictionary id actually changes (consecutive rows sharing
  // an id are the common case within a run)
  private static void consumeMixedPage(
      MixedPageReader mixedReader, long firstRow, long lastRow, GroupAccumulator accumulator) {
    ColumnReader pathReader = mixedReader.pathReader();
    Dictionary dictionary = mixedReader.dictionary();
    int lastId = -1;
    Binary lastValue = null;
    for (long row = firstRow; row <= lastRow; row++) {
      int id = pathReader.getCurrentValueDictionaryID();
      if (id != lastId) {
        lastId = id;
        lastValue = dictionary.decodeToBinary(id);
      }
      accumulator.extend(lastValue, row);
      pathReader.consume();
    }
  }

  // accumulates consecutive (value, row) observations into PathGroups, closing out the current
  // group and starting a new one whenever the value changes
  private static final class GroupAccumulator {
    private final List<PathGroup> groups = Lists.newArrayList();
    private Binary currentValue;
    private long currentLastRow = -1;

    void extend(Binary value, long lastRow) {
      if (currentValue == null || !value.equals(currentValue)) {
        if (currentValue != null) {
          groups.add(new PathGroup(currentValue.toStringUsingUTF8(), currentLastRow));
        }
        currentValue = value;
      }
      currentLastRow = lastRow;
    }

    List<PathGroup> finish() {
      if (currentValue != null) {
        groups.add(new PathGroup(currentValue.toStringUsingUTF8(), currentLastRow));
      }
      return groups;
    }
  }

  // plain pos-only read of the whole row group - positions can't be skipped, every row is needed
  // regardless - filing each one into the PositionDeleteIndex for whichever group its absolute row
  // falls under. Groups within one row group are always for distinct paths (sort order rules out
  // the same path reappearing non-adjacently), so this never needs to look up an existing entry
  private static CharSequenceMap<PositionDeleteIndex> readPosGroupedByPath(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor posDescriptor,
      int rowGroupIndex,
      List<PathGroup> groups,
      long blockRowCount,
      DeleteFile deleteFile)
      throws IOException {
    CharSequenceMap<PositionDeleteIndex> result = CharSequenceMap.create();
    if (groups.isEmpty()) {
      return result;
    }

    reader.setRequestedSchema(Collections.singletonList(posDescriptor));
    // ColumnReadStoreImpl must be given a schema matching exactly what was requested above - the
    // full 2-column schema would make it look for file_path data that was never fetched
    MessageType posOnlySchema = new MessageType(schema.getName(), schema.getType("pos"));
    PageReadStore pageReadStore = reader.readRowGroup(rowGroupIndex);
    ColumnReadStoreImpl columnReadStore =
        new ColumnReadStoreImpl(
            pageReadStore,
            ParallelPositionDeleteReader.NOOP_GROUP_CONVERTER,
            posOnlySchema,
            createdBy);
    ColumnReader posReader = columnReadStore.getColumnReader(posDescriptor);

    int groupIndex = 0;
    PositionDeleteIndex currentIndex = new BitmapPositionDeleteIndex(deleteFile);
    result.put(groups.get(0).path(), currentIndex);
    for (long r = 0; r < blockRowCount; r++) {
      if (r > groups.get(groupIndex).lastRow()) {
        groupIndex++;
        currentIndex = new BitmapPositionDeleteIndex(deleteFile);
        result.put(groups.get(groupIndex).path(), currentIndex);
      }
      currentIndex.delete(posReader.getLong());
      posReader.consume();
    }
    return result;
  }

  private static byte[] fetchWholeFile(org.apache.iceberg.io.InputFile icebergInputFile)
      throws IOException {
    int length = (int) icebergInputFile.getLength();
    byte[] data = new byte[length];
    IOUtil.readFully(icebergInputFile, 0, data, 0, length);
    return data;
  }

  private static InputFile inMemoryInputFile(byte[] data) {
    return new InputFile() {
      @Override
      public long getLength() {
        return data.length;
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() {
        return new InMemorySeekableInputStream(data);
      }
    };
  }

  // a fresh instance is handed out per newStream() call, each with its own independent position
  // over the same shared, read-only byte[] - safe for concurrent row-group tasks to read from
  private static class InMemorySeekableInputStream
      extends org.apache.parquet.io.SeekableInputStream {
    private final byte[] data;
    private int pos = 0;

    InMemorySeekableInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      this.pos = (int) newPos;
    }

    @Override
    public int read() {
      return pos < data.length ? data[pos++] & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int toRead = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, toRead);
      pos += toRead;
      return toRead;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (pos + len > data.length) {
        throw new EOFException(
            "Reached the end of stream with " + (pos + len - data.length) + " bytes left to read");
      }
      System.arraycopy(data, pos, bytes, start, len);
      pos += len;
    }

    @Override
    public int read(ByteBuffer buf) {
      if (pos >= data.length) {
        return -1;
      }
      int toRead = Math.min(buf.remaining(), data.length - pos);
      buf.put(data, pos, toRead);
      pos += toRead;
      return toRead;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      int remaining = buf.remaining();
      if (pos + remaining > data.length) {
        throw new EOFException(
            "Reached the end of stream with "
                + (pos + remaining - data.length)
                + " bytes left to read");
      }
      buf.put(data, pos, remaining);
      pos += remaining;
    }
  }
}
