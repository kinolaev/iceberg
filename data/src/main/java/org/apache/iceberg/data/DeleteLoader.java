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
package org.apache.iceberg.data;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeSet;

/** An API for loading delete file content into in-memory data structures. */
public interface DeleteLoader {
  /**
   * Loads the content of equality delete files into a set.
   *
   * @param deleteFiles equality delete files
   * @param projection a projection of columns to load
   * @return a set of equality deletes
   */
  StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection);

  /**
   * Loads the content of equality delete files into a map of delete key to the highest data
   * sequence number among the delete files that contain that key.
   *
   * <p>Unlike {@link #loadEqualityDeletes}, the sequence number lets a caller determine whether a
   * given delete actually applies to a specific data file (only deletes with a sequence number
   * greater than the data file's data sequence number apply). This is useful when the result is
   * shared across multiple data files, e.g. all the data files in a scan task group, since not
   * every delete file is necessarily applicable to every one of those data files.
   *
   * <p>The default implementation calls {@link #loadEqualityDeletes} once per delete file. Override
   * this method to provide a more efficient implementation, e.g. one that loads all delete files in
   * a single batch.
   *
   * @param deleteFiles equality delete files
   * @param projection a projection of columns to load
   * @return a map of equality delete keys to the max data sequence number of their delete files
   */
  default StructLikeMap<Long> loadEqualityDeletesBySequenceNumber(
      Iterable<DeleteFile> deleteFiles, Schema projection) {
    StructLikeMap<Long> deletes = StructLikeMap.create(projection.asStruct());
    for (DeleteFile deleteFile : deleteFiles) {
      Long dataSequenceNumber = deleteFile.dataSequenceNumber();
      Preconditions.checkArgument(
          dataSequenceNumber != null,
          "Equality delete file has no data sequence number: %s",
          deleteFile.location());
      for (StructLike key : loadEqualityDeletes(ImmutableList.of(deleteFile), projection)) {
        deletes.merge(key, dataSequenceNumber, Math::max);
      }
    }
    return deletes;
  }

  /**
   * Loads the content of a deletion vector or position delete files for a given data file path into
   * a position index.
   *
   * @param deleteFiles a deletion vector or position delete files
   * @param filePath the data file path for which to load deletes
   * @return a position delete index for the provided data file path
   */
  PositionDeleteIndex loadPositionDeletes(Iterable<DeleteFile> deleteFiles, CharSequence filePath);
}
