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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.parquet.ParquetInputFileAdapter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;

/**
 * Loads the position deletes for one target data file out of a position delete file, exploiting the
 * spec-guaranteed sort order (by path then position) to minimize how much of the delete file's
 * {@code file_path} column ever needs to be read.
 *
 * <p>After a cheap dictionary-based skip check identifies which row groups contain the target path,
 * matches are classified relative to each other, since sort order means at most the first and last
 * matching row groups can have "mixed" content:
 *
 * <ul>
 *   <li><b>middle</b> (strictly between the first and last match, only possible with 3+ matching
 *       row groups): guaranteed 100% target-path rows - no {@code file_path} read at all, straight
 *       {@code pos}-only read of the whole row group.
 *   <li><b>first</b> (of 2+ matches): the match runs from some point to the row group's own end.
 *       Page-level column-index stats narrow that "some point" to a single candidate page; a {@code
 *       file_path}-only scan of just that page (never touching {@code pos} at all) finds the exact
 *       row where the target path starts, then one {@code pos}-only read covers from there through
 *       the row group's end.
 *   <li><b>last</b> (of 2+ matches): symmetric - a {@code file_path}-only scan of the last
 *       candidate page finds where the target path stops, then one {@code pos}-only read covers
 *       from the row group's start through that row.
 *   <li><b>solo</b> (only one matching row group): both ends can be "mixed", so both the first and
 *       last candidate page (which may be the same page, found in one pass) get a {@code
 *       file_path}-only scan, then a single {@code pos}-only read covers the row range between
 *       them.
 * </ul>
 *
 * <p>Every role ends the same way: exactly one {@code pos}-only read, scoped to whichever pages the
 * boundary scan(s) narrowed things down to. Path and {@code pos} are never decoded in the same loop
 * - each read touches exactly one column, using {@code ParquetFileReader.setRequestedSchema} so the
 * other column is never fetched or decompressed for it, not just skipped during decoding.
 *
 * <p>The candidate page(s) are found directly from the column index's per-page min/max (a one-pass
 * scan - {@code ColumnIndexFilter.calculateRowRanges} would do the same per-page work internally
 * and then only hand back a merged row range, requiring a second pass to translate that back into
 * page indices). Both columns are primed into the reader's {@code ColumnIndexStore} up front,
 * since it caches itself per row group scoped to whichever columns were first requested - see the
 * priming call in {@code decodeRowGroup} for why.
 *
 * <p>Every matching row group runs as its own task on a dedicated fixed-size pool. Since different
 * row groups can never share positions, merging each task's result into the final index is a plain
 * union.
 */
public class ParallelPositionDeleteReader {

  private ParallelPositionDeleteReader() {}

  public static PositionDeleteIndex read(
      org.apache.iceberg.io.InputFile icebergInputFile,
      DeleteFile deleteFile,
      String targetPath,
      int rowGroupThreadPoolSize)
      throws IOException {
    InputFile inputFile = ParquetInputFileAdapter.of(icebergInputFile);
    Binary target = Binary.fromString(targetPath);
    // computed once as an immutable byte[], not passed as a shared Binary: Binary.getBytes() on a
    // ByteBufferBackedBinary mutates the underlying buffer's position, so sharing one Binary
    // across concurrent row-group tasks is a real data race (hit as BufferUnderflowException, not
    // just theoretical). Wrapped as a ByteBuffer so the binary search can compare directly via
    // ByteBuffer.compareTo() - safe to share (no mutation, unlike Binary.getBytes()) and avoids
    // allocating a byte[] per comparison
    ByteBuffer targetBuffer = ByteBuffer.wrap(target.getBytes());
    ParquetMetadata footer = readFooter(inputFile);
    MessageType schema = footer.getFileMetaData().getSchema();
    String createdBy = footer.getFileMetaData().getCreatedBy();
    ColumnDescriptor pathDescriptor = schema.getColumnDescription(new String[] {"file_path"});
    ColumnDescriptor posDescriptor = schema.getColumnDescription(new String[] {"pos"});
    List<BlockMetaData> blocks = footer.getBlocks();

    Matches matches = findMatches(inputFile, footer, pathDescriptor, target, blocks);
    if (matches.rowGroups().isEmpty()) {
      return new BitmapPositionDeleteIndex(new RoaringPositionBitmap(), deleteFile);
    }

    ReadContext context =
        new ReadContext(
            inputFile,
            footer,
            schema,
            createdBy,
            pathDescriptor,
            posDescriptor,
            targetBuffer,
            deleteFile);

    List<Integer> rowGroups = matches.rowGroups();
    List<Integer> targetIds = matches.targetIds();
    if (rowGroups.size() == 1) {
      // nothing to parallelize - skip the executor entirely
      return decodeRowGroup(context, targetIds.get(0), rowGroups.get(0), Role.SOLO);
    }

    int first = rowGroups.get(0);
    int last = rowGroups.get(rowGroups.size() - 1);

    ExecutorService executor =
        Executors.newFixedThreadPool(Math.min(rowGroupThreadPoolSize, rowGroups.size()));
    try {
      CompletionService<PositionDeleteIndex> completionService =
          new ExecutorCompletionService<>(executor);
      for (int m = 0; m < rowGroups.size(); m++) {
        int rowGroupIndex = rowGroups.get(m);
        int targetId = targetIds.get(m);
        Role role =
            rowGroupIndex == first ? Role.FIRST : rowGroupIndex == last ? Role.LAST : Role.MIDDLE;
        completionService.submit(() -> decodeRowGroup(context, targetId, rowGroupIndex, role));
      }

      // different row groups can never share positions, so combining results is a plain union -
      // no smaller-into-bigger merge needed, since there's no risk of the same key being touched
      // by two tasks
      PositionDeleteIndex result =
          new BitmapPositionDeleteIndex(new RoaringPositionBitmap(), deleteFile);
      for (int m = 0; m < rowGroups.size(); m++) {
        PositionDeleteIndex partial = completionService.take().get();
        result.merge(partial);
      }
      return result;
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to read position deletes for " + targetPath, e);
    } finally {
      executor.shutdown();
    }
  }

  // the row groups a dictionary-only skip check found the target path in, and the dictionary ID
  // it resolved to within each one (same index into both lists)
  private record Matches(List<Integer> rowGroups, List<Integer> targetIds) {}

  // cheap dictionary-only skip check per row group, sequential - this only reads each row group's
  // dictionary page, not its data, so parallelizing it isn't worth the complexity
  private static Matches findMatches(
      InputFile inputFile,
      ParquetMetadata footer,
      ColumnDescriptor pathDescriptor,
      Binary target,
      List<BlockMetaData> blocks)
      throws IOException {
    List<Integer> rowGroups = Lists.newArrayList();
    List<Integer> targetIds = Lists.newArrayList();
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            inputFile, footer, ParquetReadOptions.builder().build(), inputFile.newStream())) {
      for (int i = 0; i < blocks.size(); i++) {
        DictionaryPageReadStore dictionaryReader = reader.getDictionaryReader(blocks.get(i));
        DictionaryPage dictionaryPage = dictionaryReader.readDictionaryPage(pathDescriptor);
        if (dictionaryPage == null) {
          continue;
        }
        Dictionary dictionary =
            dictionaryPage.getEncoding().initDictionary(pathDescriptor, dictionaryPage);
        for (int id = 0; id <= dictionary.getMaxId(); id++) {
          if (dictionary.decodeToBinary(id).equals(target)) {
            rowGroups.add(i);
            targetIds.add(id);
            break;
          }
        }
      }
    }
    return new Matches(rowGroups, targetIds);
  }

  private enum Role {
    SOLO,
    FIRST,
    MIDDLE,
    LAST
  }

  // bundles the arguments every row-group task shares (constant across a single read() call) so
  // decodeRowGroup only needs to take the three that actually vary per task
  private record ReadContext(
      InputFile inputFile,
      ParquetMetadata footer,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      ColumnDescriptor posDescriptor,
      ByteBuffer targetBuffer,
      DeleteFile deleteFile) {}

  private static PositionDeleteIndex decodeRowGroup(
      ReadContext context, int targetId, int rowGroupIndex, Role role) {
    InputFile inputFile = context.inputFile();
    ParquetMetadata footer = context.footer();
    MessageType schema = context.schema();
    String createdBy = context.createdBy();
    ColumnDescriptor pathDescriptor = context.pathDescriptor();
    ColumnDescriptor posDescriptor = context.posDescriptor();
    ByteBuffer targetBuffer = context.targetBuffer();
    DeleteFile deleteFile = context.deleteFile();
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            inputFile, footer, ParquetReadOptions.builder().build(), inputFile.newStream())) {
      BlockMetaData block = footer.getBlocks().get(rowGroupIndex);
      long blockRowCount = block.getRowCount();

      if (role == Role.MIDDLE) {
        // guaranteed 100% target-path rows by sort order - no path column read at all
        return readPosOnly(
            reader,
            schema,
            createdBy,
            posDescriptor,
            rowGroupIndex,
            null,
            0,
            0,
            blockRowCount,
            deleteFile);
      }

      ColumnChunkMetaData pathColumn = null;
      ColumnChunkMetaData posColumn = null;
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getPath().toDotString().equals("file_path")) {
          pathColumn = column;
        } else if (column.getPath().toDotString().equals("pos")) {
          posColumn = column;
        }
      }

      // prime the reader's ColumnIndexStore cache for this row group with both columns before
      // any restricted setRequestedSchema call - it's created (and cached) on first touch, scoped
      // to whatever columns were requested then, and every filtered read below (whether
      // file_path-only or pos-only, in either order depending on role) needs both columns'
      // offset indexes to already be in it. This same call already eagerly fetches both columns'
      // offset indexes, so the path column's offset index is read from it below rather than via a
      // second, duplicate direct read
      reader.setRequestedSchema(Arrays.asList(pathDescriptor, posDescriptor));
      ColumnIndexStore ciStore = reader.getColumnIndexStore(rowGroupIndex);

      ColumnIndex columnIndex = ciStore.getColumnIndex(pathColumn.getPath());
      OffsetIndex offsetIndex = ciStore.getOffsetIndex(pathColumn.getPath());
      // getMinValues()/getMaxValues() rebuild their entire list - decoding every page's value - on
      // every call (see ColumnIndexBuilder.ColumnIndexBase); getNullPages() is a cheap array wrap,
      // used here just for the count. Both lists are fetched once and reused below instead of
      // rebuilt per binary-search comparison - O(pageCount) instead of O(pageCount log pageCount)
      // for a row group with thousands of pages
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      int pageCount = columnIndex.getNullPages().size();

      // per-page min/max are monotonically non-decreasing across the row group, since delete
      // files are sorted by (path, pos) - binary search for the bracketing page(s) instead of a
      // linear scan (a linear scan over every page, most of which are irrelevant, dominated the
      // cost of the whole lookup: ~600ms out of ~700ms for a 5000-page row group). FIRST/LAST
      // only need one binary search each: the other boundary is already known - FIRST's match
      // necessarily runs to the row group's own last page (it continues into a later matching
      // row group), and LAST's necessarily starts at page 0 (it continues from an earlier one).
      // Only SOLO, with no adjacent matching row group on either side, needs both
      BoundaryContext boundaryContext =
          new BoundaryContext(
              reader, schema, createdBy, pathDescriptor, targetId, rowGroupIndex, offsetIndex);
      long[] bounds =
          switch (role) {
            case FIRST -> resolveFirstBounds(
                boundaryContext, minValues, maxValues, pageCount, targetBuffer, blockRowCount);
            case LAST -> resolveLastBounds(
                boundaryContext, minValues, maxValues, pageCount, targetBuffer, blockRowCount);
            case SOLO -> resolveSoloBounds(
                boundaryContext, minValues, maxValues, pageCount, targetBuffer, blockRowCount);
            default -> throw new IllegalStateException("Unexpected role: " + role);
          };
      long start = bounds[0];
      long end = bounds[1];

      // scope the fetch to pos's own pages covering [start, end), not path's - the two columns
      // generally have different page boundaries (page splits are usually byte-size-based, and
      // path/pos rows are rarely the same size), so bounding the pos fetch by path's pages would
      // either over-fetch or under-fetch relative to what pos itself actually needs. pos's offset
      // index is already free here - it was eagerly fetched by the same priming call above
      OffsetIndex posOffsetIndex = ciStore.getOffsetIndex(posColumn.getPath());
      int posPageCount = posOffsetIndex.getPageCount();
      int posFirstPage = pageContainingRow(posOffsetIndex, 0, posPageCount, start);
      // end - 1 >= start always, so the page containing it can never be before posFirstPage -
      // narrow the search instead of starting over from page 0
      int posLastPage = pageContainingRow(posOffsetIndex, posFirstPage, posPageCount, end - 1);
      RowRanges range = pageRange(posFirstPage, posLastPage, posOffsetIndex, blockRowCount);
      long firstFetchedRow = posOffsetIndex.getFirstRowIndex(posFirstPage);
      return readPosOnly(
          reader,
          schema,
          createdBy,
          posDescriptor,
          rowGroupIndex,
          range,
          firstFetchedRow,
          start,
          end,
          deleteFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // bundles the arguments resolveFirstBounds/resolveLastBounds/resolveSoloBounds all share, on
  // top of the per-role page-index inputs each already takes
  private record BoundaryContext(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      int targetId,
      int rowGroupIndex,
      OffsetIndex offsetIndex) {}

  private static long[] resolveFirstBounds(
      BoundaryContext context,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      int pageCount,
      ByteBuffer targetBuffer,
      long blockRowCount)
      throws IOException {
    int firstCandidate = firstPageWithMaxAtLeast(maxValues, pageCount, targetBuffer);
    if (firstCandidate >= pageCount) {
      // shouldn't happen - dictionary already confirmed the target is present
      firstCandidate = 0;
    }
    // if the candidate page's own min is already target, sortedness plus the FIRST-role
    // guarantee (once target starts in this row group it runs through the row group's end)
    // means the whole page - and everything after it - is target: no path read needed for this
    // row group at all
    long start =
        isTarget(minValues.get(firstCandidate), targetBuffer)
            ? context.offsetIndex().getFirstRowIndex(firstCandidate)
            : findStart(
                context.reader(),
                context.schema(),
                context.createdBy(),
                context.pathDescriptor(),
                context.targetId(),
                context.rowGroupIndex(),
                firstCandidate,
                context.offsetIndex(),
                blockRowCount);
    return new long[] {start, blockRowCount};
  }

  private static long[] resolveLastBounds(
      BoundaryContext context,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      int pageCount,
      ByteBuffer targetBuffer,
      long blockRowCount)
      throws IOException {
    int lastCandidate = lastPageWithMinAtMost(minValues, 0, pageCount, targetBuffer);
    if (lastCandidate < 0) {
      // shouldn't happen - dictionary already confirmed the target is present
      lastCandidate = pageCount - 1;
    }
    // symmetric to FIRST: if the candidate page's own max is already target, the whole page -
    // and everything before it - is target
    long end =
        isTarget(maxValues.get(lastCandidate), targetBuffer)
            ? context.offsetIndex().getLastRowIndex(lastCandidate, blockRowCount) + 1
            : findEnd(
                context.reader(),
                context.schema(),
                context.createdBy(),
                context.pathDescriptor(),
                context.targetId(),
                context.rowGroupIndex(),
                lastCandidate,
                context.offsetIndex(),
                blockRowCount);
    return new long[] {0, end};
  }

  private static long[] resolveSoloBounds(
      BoundaryContext context,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      int pageCount,
      ByteBuffer targetBuffer,
      long blockRowCount)
      throws IOException {
    int firstCandidate = firstPageWithMaxAtLeast(maxValues, pageCount, targetBuffer);
    // the last matching page can never be before firstCandidate: every page before it has
    // max < target, and since min <= max, those pages trivially satisfy min <= target too, but
    // firstCandidate itself already satisfies min <= target (that's where target's
    // dictionary-confirmed occurrence actually starts) - so searching from firstCandidate onward
    // still finds the correct answer, just faster
    int lastCandidate =
        lastPageWithMinAtMost(
            minValues, Math.min(firstCandidate, pageCount - 1), pageCount, targetBuffer);
    if (firstCandidate > lastCandidate || firstCandidate >= pageCount || lastCandidate < 0) {
      // shouldn't happen - dictionary already confirmed the target is present
      firstCandidate = 0;
      lastCandidate = pageCount - 1;
    }
    // same page-homogeneity check as FIRST/LAST, applied to each boundary independently - SOLO
    // has no adjacent matching row group to lean on, so neither side can be assumed without its
    // own page checking its own min/max. Whenever one side does resolve directly, it also
    // guarantees the other candidate page's own boundary row is already target (its run can't
    // have ended before reaching that page, or the other side wouldn't be a candidate at all),
    // so the existing single-page findStart/findEnd already have the precondition they need for
    // the unresolved side
    boolean startResolved = isTarget(minValues.get(firstCandidate), targetBuffer);
    boolean endResolved = isTarget(maxValues.get(lastCandidate), targetBuffer);
    OffsetIndex offsetIndex = context.offsetIndex();
    if (startResolved && endResolved) {
      return new long[] {
        offsetIndex.getFirstRowIndex(firstCandidate),
        offsetIndex.getLastRowIndex(lastCandidate, blockRowCount) + 1
      };
    } else if (startResolved) {
      return new long[] {
        offsetIndex.getFirstRowIndex(firstCandidate),
        findEnd(
            context.reader(),
            context.schema(),
            context.createdBy(),
            context.pathDescriptor(),
            context.targetId(),
            context.rowGroupIndex(),
            lastCandidate,
            offsetIndex,
            blockRowCount)
      };
    } else if (endResolved) {
      return new long[] {
        findStart(
            context.reader(),
            context.schema(),
            context.createdBy(),
            context.pathDescriptor(),
            context.targetId(),
            context.rowGroupIndex(),
            firstCandidate,
            offsetIndex,
            blockRowCount),
        offsetIndex.getLastRowIndex(lastCandidate, blockRowCount) + 1
      };
    } else if (firstCandidate == lastCandidate) {
      // both ends fall in the same page and neither resolves directly - one pass finds them
      // together
      return findBounds(
          context.reader(),
          context.schema(),
          context.createdBy(),
          context.pathDescriptor(),
          context.targetId(),
          context.rowGroupIndex(),
          firstCandidate,
          offsetIndex,
          blockRowCount);
    } else {
      // one combined read of both boundary pages instead of two separate findStart/findEnd
      // calls: file_path is dictionary-encoded, and the dictionary page isn't cached across
      // separate readFilteredRowGroup calls, so two calls would mean decoding it twice
      return findStartAndEnd(
          context.reader(),
          context.schema(),
          context.createdBy(),
          context.pathDescriptor(),
          context.targetId(),
          context.rowGroupIndex(),
          firstCandidate,
          lastCandidate,
          offsetIndex,
          blockRowCount);
    }
  }

  private static RowRanges pageRange(
      int fromPage, int toPage, OffsetIndex offsetIndex, long blockRowCount) {
    return RowRanges.create(
        blockRowCount, IntStream.rangeClosed(fromPage, toPage).iterator(), offsetIndex);
  }

  // binary search for the page containing targetRow (searching from fromPage onward), using a
  // column's own per-page row boundaries (monotonically increasing by construction - every
  // column's pages cover the row group's rows in order, regardless of how many pages that column
  // happens to be split into)
  private static int pageContainingRow(
      OffsetIndex offsetIndex, int fromPage, int pageCount, long targetRow) {
    int lo = fromPage;
    int hi = pageCount - 1;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (offsetIndex.getFirstRowIndex(mid) <= targetRow) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  // first page index p such that max[p] >= target, i.e. the earliest page whose range could
  // still reach target from below. Returns pageCount if no such page exists. Compares the raw
  // ByteBuffers directly - no need to go through Binary at all: ByteBuffer.compareTo() already
  // does the same unsigned lexicographic comparison as Arrays.compare(byte[], byte[]), without
  // allocating or copying a byte[] out of every candidate page's min/max value
  private static int firstPageWithMaxAtLeast(
      List<ByteBuffer> maxValues, int pageCount, ByteBuffer targetBuffer) {
    int lo = 0;
    int hi = pageCount - 1;
    int result = pageCount;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (maxValues.get(mid).compareTo(targetBuffer) >= 0) {
        result = mid;
        hi = mid - 1;
      } else {
        lo = mid + 1;
      }
    }
    return result;
  }

  // last page index p (searching from fromPage onward) such that min[p] <= target, i.e. the
  // latest page whose range could still reach target from above. Returns -1 if no such page
  // exists in [fromPage, pageCount)
  private static int lastPageWithMinAtMost(
      List<ByteBuffer> minValues, int fromPage, int pageCount, ByteBuffer targetBuffer) {
    int lo = fromPage;
    int hi = pageCount - 1;
    int result = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (minValues.get(mid).compareTo(targetBuffer) <= 0) {
        result = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return result;
  }

  // true if a page's own min or max value is exactly target, not just >=/<= it - used to detect a
  // page (or a whole boundary, combined with role/sortedness guarantees) that's already known to
  // be 100% target without any row-level scan
  private static boolean isTarget(ByteBuffer pageValue, ByteBuffer targetBuffer) {
    return pageValue.compareTo(targetBuffer) == 0;
  }

  // file_path-only scan of one page from its own start - returns the absolute row index of the
  // first row where the target path appears, as soon as it's found. pos is never touched here -
  // only file_path is fetched or decompressed
  private static long findStart(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      int targetId,
      int rowGroupIndex,
      int page,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    PageReadStore pageReadStore =
        openPathOnlyPage(reader, pathDescriptor, rowGroupIndex, page, offsetIndex, blockRowCount);
    ColumnReader pathReader =
        pathOnlyColumnReader(pageReadStore, schema, createdBy, pathDescriptor);
    long firstRow = offsetIndex.getFirstRowIndex(page);
    long rowCount = pageReadStore.getRowCount();
    for (long r = 0; r < rowCount; r++) {
      if (pathReader.getCurrentValueDictionaryID() == targetId) {
        return firstRow + r;
      }
      pathReader.consume();
    }
    return firstRow; // shouldn't happen - page was a bracket candidate
  }

  // file_path-only scan of one page from its own start - the page is known to start as 100%
  // target-path rows (by role/contiguous-row-group reasoning), so this returns the absolute row
  // index of the first row where it stops, as soon as that's found (or the page's own end if the
  // whole page turns out to be target - possible when target's data ends exactly there)
  private static long findEnd(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      int targetId,
      int rowGroupIndex,
      int page,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    PageReadStore pageReadStore =
        openPathOnlyPage(reader, pathDescriptor, rowGroupIndex, page, offsetIndex, blockRowCount);
    ColumnReader pathReader =
        pathOnlyColumnReader(pageReadStore, schema, createdBy, pathDescriptor);
    long firstRow = offsetIndex.getFirstRowIndex(page);
    long rowCount = pageReadStore.getRowCount();
    for (long r = 0; r < rowCount; r++) {
      if (pathReader.getCurrentValueDictionaryID() != targetId) {
        return firstRow + r;
      }
      pathReader.consume();
    }
    return firstRow + rowCount;
  }

  // combined file_path-only scan of two distinct boundary pages in one read - used for a SOLO
  // match whose start and end fall in different pages, to avoid decoding file_path's dictionary
  // page twice (it isn't cached across separate readFilteredRowGroup calls). Everything strictly
  // between the two pages is guaranteed pure target by the same reasoning as MIDDLE row groups,
  // so once "start" is found within firstPage, the scan only needs to watch for "end" once it
  // reaches lastPage's own rows - returning as soon as both are found
  private static long[] findStartAndEnd(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      int targetId,
      int rowGroupIndex,
      int firstPage,
      int lastPage,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    RowRanges range =
        RowRanges.create(blockRowCount, IntStream.of(firstPage, lastPage).iterator(), offsetIndex);
    PageReadStore pageReadStore = openPathOnlyRange(reader, pathDescriptor, rowGroupIndex, range);
    ColumnReader pathReader =
        pathOnlyColumnReader(pageReadStore, schema, createdBy, pathDescriptor);

    long firstPageFirstRow = offsetIndex.getFirstRowIndex(firstPage);
    long firstPageRowCount =
        offsetIndex.getLastRowIndex(firstPage, blockRowCount) - firstPageFirstRow + 1;
    long lastPageFirstRow = offsetIndex.getFirstRowIndex(lastPage);
    long totalRowCount = pageReadStore.getRowCount();

    // find start: scan from firstPage's own first row until the target path appears
    long row = 0;
    long start = -1;
    for (; row < totalRowCount; row++) {
      if (pathReader.getCurrentValueDictionaryID() == targetId) {
        start =
            row < firstPageRowCount
                ? firstPageFirstRow + row
                : lastPageFirstRow + (row - firstPageRowCount);
        break;
      }
      pathReader.consume();
    }
    if (start < 0) {
      // shouldn't happen - firstPage was a bracket candidate
      return new long[] {firstPageFirstRow, firstPageFirstRow};
    }
    pathReader.consume(); // move past the row where start was found

    // find end: everything from start through lastPage's own transition is guaranteed
    // target-matching (same reasoning as MIDDLE row groups - firstCandidate/lastCandidate are the
    // only two pages that can be "mixed"), so this only needs to look for the first non-match
    long end = -1;
    for (row = row + 1; row < totalRowCount; row++) {
      if (pathReader.getCurrentValueDictionaryID() != targetId) {
        end = lastPageFirstRow + (row - firstPageRowCount);
        break;
      }
      pathReader.consume();
    }
    if (end < 0) {
      end = lastPageFirstRow + (totalRowCount - firstPageRowCount); // lastPage's own end
    }
    return new long[] {start, end};
  }

  // file_path-only scan of one page - used only when a SOLO match's start and end both fall in
  // the same page: finds where the target path's data starts and stops within it, returning as
  // soon as both are found
  private static long[] findBounds(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor,
      int targetId,
      int rowGroupIndex,
      int page,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    PageReadStore pageReadStore =
        openPathOnlyPage(reader, pathDescriptor, rowGroupIndex, page, offsetIndex, blockRowCount);
    ColumnReader pathReader =
        pathOnlyColumnReader(pageReadStore, schema, createdBy, pathDescriptor);
    long firstRow = offsetIndex.getFirstRowIndex(page);
    long rowCount = pageReadStore.getRowCount();

    // find start: scan from the page's own first row until the target path appears
    long row = 0;
    long start = -1;
    for (; row < rowCount; row++) {
      if (pathReader.getCurrentValueDictionaryID() == targetId) {
        start = firstRow + row;
        break;
      }
      pathReader.consume();
    }
    if (start < 0) {
      // shouldn't happen - page was a bracket candidate
      return new long[] {firstRow, firstRow};
    }
    pathReader.consume(); // move past the row where start was found

    // find end: scan onward for the first row where the target path stops
    long end = -1;
    for (row = row + 1; row < rowCount; row++) {
      if (pathReader.getCurrentValueDictionaryID() != targetId) {
        end = firstRow + row;
        break;
      }
      pathReader.consume();
    }
    if (end < 0) {
      end = firstRow + rowCount; // target's data runs to (at least) this page's own end
    }
    return new long[] {start, end};
  }

  private static PageReadStore openPathOnlyPage(
      ParquetFileReader reader,
      ColumnDescriptor pathDescriptor,
      int rowGroupIndex,
      int page,
      OffsetIndex offsetIndex,
      long blockRowCount)
      throws IOException {
    RowRanges range = pageRange(page, page, offsetIndex, blockRowCount);
    return openPathOnlyRange(reader, pathDescriptor, rowGroupIndex, range);
  }

  // package-private: also used by ParallelPositionDeleteCacheLoader
  static PageReadStore openPathOnlyRange(
      ParquetFileReader reader, ColumnDescriptor pathDescriptor, int rowGroupIndex, RowRanges range)
      throws IOException {
    reader.setRequestedSchema(Collections.singletonList(pathDescriptor));
    return reader.readFilteredRowGroup(rowGroupIndex, range);
  }

  // package-private: also used by ParallelPositionDeleteCacheLoader
  static ColumnReader pathOnlyColumnReader(
      PageReadStore pageReadStore,
      MessageType schema,
      String createdBy,
      ColumnDescriptor pathDescriptor) {
    // ColumnReadStoreImpl must be given a schema matching exactly what setRequestedSchema
    // restricted the reader to - the full 2-column schema would make it look for pos data that
    // was never fetched
    MessageType pathOnlySchema = new MessageType(schema.getName(), schema.getType("file_path"));
    ColumnReadStoreImpl columnReadStore =
        new ColumnReadStoreImpl(pageReadStore, NOOP_GROUP_CONVERTER, pathOnlySchema, createdBy);
    return columnReadStore.getColumnReader(pathDescriptor);
  }

  // pos-only read, discarding rows outside [start, end) - pages are the physical fetch/decode
  // granularity, so a boundary page can't be fetched partially; every fetched row's value must
  // still be decoded (not just skipped) to keep the decoder's cursor in sync for the rows that do
  // matter. pageRange == null means the whole row group (used for guaranteed-pure middle row
  // groups, where start/end are simply 0/blockRowCount and the check is a no-op)
  private static PositionDeleteIndex readPosOnly(
      ParquetFileReader reader,
      MessageType schema,
      String createdBy,
      ColumnDescriptor posDescriptor,
      int rowGroupIndex,
      RowRanges pageRange,
      long firstFetchedRow,
      long start,
      long end,
      DeleteFile deleteFile)
      throws IOException {
    reader.setRequestedSchema(Collections.singletonList(posDescriptor));
    // ColumnReadStoreImpl must be given a schema matching exactly what was requested above - the
    // full 2-column schema would make it look for file_path data that was never fetched
    MessageType posOnlySchema = new MessageType(schema.getName(), schema.getType("pos"));
    PageReadStore pageReadStore =
        pageRange == null
            ? reader.readRowGroup(rowGroupIndex)
            : reader.readFilteredRowGroup(rowGroupIndex, pageRange);
    ColumnReadStoreImpl columnReadStore =
        new ColumnReadStoreImpl(pageReadStore, NOOP_GROUP_CONVERTER, posOnlySchema, createdBy);
    ColumnReader posReader = columnReadStore.getColumnReader(posDescriptor);
    PositionDeleteIndex result =
        new BitmapPositionDeleteIndex(new RoaringPositionBitmap(), deleteFile);
    long rowCount = pageReadStore.getRowCount();
    for (long r = 0; r < rowCount; r++) {
      long pos = posReader.getLong();
      long absoluteRow = firstFetchedRow + r;
      if (absoluteRow >= start && absoluteRow < end) {
        result.delete(pos);
      }
      posReader.consume();
    }
    return result;
  }

  // package-private: also used by ParallelPositionDeleteCacheLoader
  static ParquetMetadata readFooter(InputFile inputFile) throws IOException {
    try (ParquetFileReader footerReader = ParquetFileReader.open(inputFile)) {
      return footerReader.getFooter();
    }
  }

  // dictionary-ID-based readers compare only integer IDs, never materializing a file_path
  // Binary/String, to avoid Binary.equals() byte comparisons (no identity fast path even for
  // values reused via dictionary encoding). This no-op GroupConverter is required for that:
  // ColumnReader.getCurrentValueDictionaryID() throws UnsupportedOperationException unless the
  // bound PrimitiveConverter reports dictionary support.
  // package-private: also used by ParallelPositionDeleteCacheLoader
  static final GroupConverter NOOP_GROUP_CONVERTER =
      new GroupConverter() {
        private final PrimitiveConverter noopConverter =
            new PrimitiveConverter() {
              @Override
              public void addBinary(Binary value) {}

              @Override
              public void addLong(long value) {}

              @Override
              public boolean hasDictionarySupport() {
                return true;
              }

              @Override
              public void setDictionary(Dictionary dictionary) {}

              @Override
              public void addValueFromDictionary(int dictionaryId) {}
            };

        @Override
        public Converter getConverter(int fieldIndex) {
          return noopConverter;
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
      };
}
