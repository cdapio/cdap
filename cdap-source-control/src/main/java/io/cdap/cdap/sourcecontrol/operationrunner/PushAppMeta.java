/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import java.util.Objects;

/**
 * Container class for capturing meta information for a single pushed pipeline.
 */
public class PushAppMeta {
  private final String name;
  private final String version;
  private final String fileHash;

  /**
   * Default constructor for PushAppMeta.
   *
   * @param name name of the application to be pushed
   * @param version version of the application to be pushed
   * @param fileHash the git-hash of the config in git after push
   */
  public PushAppMeta(String name, String version, String fileHash) {
    this.name = name;
    this.version = version;
    this.fileHash = fileHash;
  }

  public String getName() {
    return name;
  }

  public String getFileHash() {
    return fileHash;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PushAppMeta that = (PushAppMeta) o;
    return Objects.equals(name, that.name)
        && Objects.equals(version, that.version)
        && Objects.equals(fileHash, that.fileHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, fileHash);
  }
}
