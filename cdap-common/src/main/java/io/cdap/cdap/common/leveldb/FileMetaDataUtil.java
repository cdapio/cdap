/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.leveldb;

import javax.annotation.Nullable;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.util.Slice;

/**
 * Utility class for trimming down storage of keys passed to {@link org.iq80.leveldb.impl.FileMetaData#FileMetaData}
 */
public final class FileMetaDataUtil {

  private FileMetaDataUtil() {
  }

  /**
   * Check if value holds a reference to a big array (more than needed to store a key) and create a
   * copy of the key with minimum storage needed if so.
   *
   * @param value original key to be analyzed
   * @return original key of it uses minimum storage, copy of the key if storage had to be trimmed,
   *     null if input is null
   */
  @Nullable
  public static InternalKey normalize(@Nullable InternalKey value) {
    if (value == null) {
      return null;
    }
    Slice userKey = value.getUserKey();
    if (userKey.getRawArray().length <= userKey.length()) {
      return value;
    }
    return new InternalKey(value.getUserKey().copySlice(), value.getSequenceNumber(),
        value.getValueType());
  }
}
