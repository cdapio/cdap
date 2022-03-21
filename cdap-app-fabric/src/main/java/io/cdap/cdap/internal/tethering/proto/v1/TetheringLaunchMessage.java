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

package io.cdap.cdap.internal.tethering.proto.v1;

import io.cdap.cdap.internal.tethering.TetheringControlMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Contents of control message for {@link TetheringControlMessage.Type#START_PROGRAM} jobs
 */
public class TetheringLaunchMessage {
  // Map of file names and its compressed contents
  private final Map<String, byte[]> localizeFiles;
  // Select cConf entries
  private final Map<String, String> cConfEntries;
  // Namespace to run program on
  private final String namespace;

  private TetheringLaunchMessage(Map<String, byte[]> localizeFiles, Map<String, String> cConfEntries,
                                 String namespace) {
    this.localizeFiles = localizeFiles;
    this.cConfEntries = cConfEntries;
    this.namespace = namespace;
  }

  public Map<String, byte[]> getFiles() {
    return localizeFiles;
  }

  public Map<String, String> getCConfEntries() {
    return cConfEntries;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String toString() {
    return "TetheringLaunchMessage{" +
      "localizeFiles='" + localizeFiles + '\'' +
      ", cConfEntries=" + cConfEntries +
      ", namespace=" + namespace +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TetheringLaunchMessage that = (TetheringLaunchMessage) o;
    return Objects.equals(localizeFiles, that.localizeFiles) &&
      Objects.equals(cConfEntries, that.cConfEntries) &&
      Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(localizeFiles, cConfEntries);
  }

  /**
   * Builder for TetheringLaunchMessage
   */
  public static final class Builder {
    private final Set<String> fileNames = new HashSet<>();
    private final Map<String, byte[]> localizeFiles = new HashMap<>();
    private final Map<String, String> cConfEntries = new HashMap<>();
    private String namespace;

    public Builder addFileNames(String fileName) {
      this.fileNames.add(fileName);
      return this;
    }

    public void addLocalizeFiles(String fileName, byte[] fileContents) {
      this.localizeFiles.put(fileName, fileContents);
    }

    public void addCConfEntries(Map<String, String> entries) {
      this.cConfEntries.putAll(entries);
    }

    public void addNamespace(String namespace) {
      this.namespace = namespace;
    }

    public Set<String> getFileNames() {
      return fileNames;
    }

    public TetheringLaunchMessage build() {
      return new TetheringLaunchMessage(localizeFiles, cConfEntries, namespace);
    }
  }
}
