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

package io.cdap.cdap.runtime.spi.launcher;

import java.net.URI;
import java.util.Objects;

/**
 * Represents file to be used while launching a job via {@link Launcher}.
 */
public class LauncherFile {
  private final String name;
  private final URI uri;
  private final boolean isArchive;

  public LauncherFile(String name, URI uri, boolean isArchive) {
    this.name = name;
    this.uri = uri;
    this.isArchive = isArchive;
  }

  /**
   * Returns name of the file.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns uri of the file.
   */
  public URI getUri() {
    return uri;
  }

  /**
   * Returns true if file is archive.
   */
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
    LauncherFile that = (LauncherFile) o;
    return isArchive == that.isArchive && name.equals(that.name) && uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, uri, isArchive);
  }
}
