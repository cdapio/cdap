/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto.metadata;

/**
 * Supported types for metadata search.
 */
public enum MetadataSearchTargetType {
  // the custom values are required because these value match the entitiy-type stored as
  // a part of MDS key.
  ALL("All"),
  ARTIFACT("Artifact"),
  APP("Application"),
  PROGRAM("Program"),
  DATASET("DatasetInstance"),
  STREAM("Stream"),
  VIEW("View");

  private final String serializedForm;

  MetadataSearchTargetType(String serializedForm) {
    this.serializedForm = serializedForm;
  }

  /**
   * @return {@link MetadataSearchTargetType} of the given value.
   */
  public static MetadataSearchTargetType valueOfSerializedForm(String value) {
    for (MetadataSearchTargetType metadataSearchTargetType : values()) {
      if (metadataSearchTargetType.serializedForm.equalsIgnoreCase(value)) {
        return metadataSearchTargetType;
      }
    }
    throw new IllegalArgumentException(String.format("No enum constant for serialized form: %s", value));
  }
}
