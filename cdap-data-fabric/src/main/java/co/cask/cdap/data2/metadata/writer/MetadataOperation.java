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

import javax.annotation.Nullable;

import static co.cask.cdap.data2.metadata.writer.MetadataOperation.Type.DELETE;
import static co.cask.cdap.data2.metadata.writer.MetadataOperation.Type.PUT;

/**
 * Represents a meta data operation for an entity.
 * <ul>
 * <li>
 * For a {@link Type#PUT}, the metadata to be added is given as a {@link Metadata} object.
 * </li><li>
 * For a {@link Type#DELETE}, the {@link Metadata} contains the tags to be deleted, and an
 * entry in the properties for each property to be deleted (the values are
 * ignored).
 * </li>
 * <li>
 * For {@link Type#DELETE_ALL} all of the properties and tags will be deleted
 * </li>
 * <li>
 * For {@link Type#DELETE_ALL_TAGS} all of the tags will be deleted
 * </li>
 * <li>
 * For {@link Type#DELETE_ALL_PROPERTIES} all of the properties will be deleted
 * </li>
 * </ul>
 */
public class MetadataOperation {

  /**
   * Type of the operation:
   * <ul><li>
   * {@link Type#PUT} for adding metadata;
   * </li><li>
   * {@link Type#DELETE} for removing metadata.
   * </li><li>
   * {@link Type#DELETE_ALL} to delete all metadata (properties and tags)
   * </li><li>
   * {@link Type#DELETE_ALL_PROPERTIES} to delete all properties
   * </li><li>
   * {@link Type#DELETE_ALL_TAGS} to delete all tags
   * </li></ul>
   */
  public enum Type {
    PUT, DELETE, DELETE_ALL, DELETE_ALL_PROPERTIES, DELETE_ALL_TAGS
  }

  private final MetadataEntity entity;
  private final Type type;
  @Nullable
  private final Metadata metadata;

  public MetadataOperation(MetadataEntity entity, Type type, @Nullable Metadata metadata) {
    if (metadata == null && (type == PUT || type == DELETE)) {
      throw new IllegalArgumentException("Metadata cannot be null");
    }
    this.entity = entity;
    this.type = type;
    this.metadata = metadata;
  }

  /**
   * @return the {@link MetadataEntity} on which this operation is performed
   */
  public MetadataEntity getEntity() {
    return entity;
  }

  /**
   * @return the {@link Type} of operation
   */
  public Type getType() {
    return type;
  }

  /**
   * @return return the {@link Metadata} for {@link Type#PUT} and {@link Type#DELETE} and null for other operations
   */
  @Nullable
  public Metadata getMetadata() {
    return metadata;
  }
}
