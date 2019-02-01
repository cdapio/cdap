/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.logbuffer;

import java.util.Comparator;
import java.util.Objects;

/**
 *
 */
public class FileOffset implements Comparable<FileOffset> {
  private static final Comparator<FileOffset> COMPARATOR = Comparator.comparing(FileOffset::getFileId)
    .thenComparing(FileOffset::getFilePos);

  private final String fileId;
  private final long filePos;

  public FileOffset(String fileId, long filePos) {
    this.fileId = fileId;
    this.filePos = filePos;
  }

  public String getFileId() {
    return fileId;
  }

  public long getFilePos() {
    return filePos;
  }

  @Override
  public int compareTo(FileOffset o) {
    return COMPARATOR.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FileOffset that = (FileOffset) o;

    return filePos == that.filePos && Objects.equals(fileId, that.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileId, filePos);
  }

  @Override
  public String toString() {
    return "FileOffset{" +
      "fileId='" + fileId + '\'' +
      ", filePos=" + filePos +
      '}';
  }
}
