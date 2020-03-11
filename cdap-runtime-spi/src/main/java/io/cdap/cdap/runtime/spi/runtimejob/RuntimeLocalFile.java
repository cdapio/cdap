/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import java.net.URI;
import java.util.Objects;

/**
 * Represents runtime local files.
 */
public class RuntimeLocalFile {
  private final String name;
  private final URI fileUri;
  private final boolean isArchive;

  public RuntimeLocalFile(String name, URI fileUri, boolean isArchive) {
    this.name = name;
    this.fileUri = fileUri;
    this.isArchive = isArchive;
  }

  public String getName() {
    return name;
  }

  public URI getFileUri() {
    return fileUri;
  }

  public boolean isArchive() {
    return isArchive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuntimeLocalFile that = (RuntimeLocalFile) o;
    return isArchive == that.isArchive && name.equals(that.name) && fileUri.equals(that.fileUri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fileUri, isArchive);
  }
}
