/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;

/**
 * Represents a meta data operation - either a put or a delete - for an entity.
 * <ul>
 *   <li>
 *     For a put, the metadata to be added is given as a {@link Metadata} object.
 *   </li><li>
 *     For a delete, the {@link Metadata} contains the tags to be deleted, and an
 *     entry in the properties for each property to be deleted (the values are
 *     ignored).
 *   </li>
 * </ul>
 */
public class MetadataOperation {

  /**
   * Type of the operation:
   * <ul><li>
   *   PUT for adding metadata;
   * </li><li>
   *   DELETE for removing metadata.
   * </li></ul>
   */
  public enum Type { PUT, DELETE }

  private final MetadataEntity entity;
  private final Type type;
  private final Metadata metadata;

  public MetadataOperation(MetadataEntity entity, Type type, Metadata metadata) {
    this.entity = entity;
    this.type = type;
    this.metadata = metadata;
  }

  public MetadataEntity getEntity() {
    return entity;
  }

  public Type getType() {
    return type;
  }

  public Metadata getMetadata() {
    return metadata;
  }
}
