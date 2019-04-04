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

package io.cdap.cdap.common.metadata;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Exception that represents a conflict when updating metadata. The message of this exception can
 * contain the string "${conflicting}", which will be replaced with the list of conflicting entities
 * when the message is obtained.
 */
public class MetadataConflictException extends IOException implements HttpErrorStatusProvider {

  private final List<MetadataEntity> conflictingEntities;

  /**
   * @param message the error message. If it contains the string "${conflicting}", it will be replaced
   *                with the conflicting entity.
   * @param conflictingEntity the entity that has a conflict
   */
  public MetadataConflictException(String message, MetadataEntity conflictingEntity) {
    this(message, Collections.singletonList(conflictingEntity));
  }

  /**
   * @param message the error message. If it contains the string "${conflicting}", it will be replaced
   *                with the list of conflicting entities.
   * @param conflictingEntities the entities that have a conflict
   */
  public MetadataConflictException(String message, List<MetadataEntity> conflictingEntities) {
    super(message);
    this.conflictingEntities = conflictingEntities;
  }

  /**
   * @return the message iwthout
   */
  public String getRawMessage() {
    return super.getMessage();
  }

  public List<MetadataEntity> getConflictingEntities() {
    return conflictingEntities;
  }

  @Override
  public String getMessage() {
    return super.getMessage().replace("${conflicting}", String.valueOf(conflictingEntities));
  }

  @Override
  public int getStatusCode() {
    return HttpResponseStatus.CONFLICT.code();
  }
}
