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
package io.cdap.cdap.common;

import io.cdap.cdap.api.metadata.MetadataEntity;

/**
 * Base exception for Metadata validation.
 */
public class InvalidMetadataException extends BadRequestException {

  private final MetadataEntity metadataEntity;

  public InvalidMetadataException(MetadataEntity metadataEntity, String message) {
    super(String.format("Unable to set metadata for %s. %s", metadataEntity.getDescription(),
        message));
    this.metadataEntity = metadataEntity;
  }

  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }
}
