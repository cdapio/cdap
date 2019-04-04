/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.kafka;

import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.NamespaceLoggingContext;
import io.cdap.cdap.logging.context.ApplicationLoggingContext;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Type of Log Partition
 */
public enum LogPartitionType {
  PROGRAM,
  APPLICATION;

  /**
   * Computes a partition key based on the given {@link LoggingContext}.
   */
  public String getPartitionKey(LoggingContext loggingContext) {
    String namespaceId = loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue();

    if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)) {
      return loggingContext.getLogPartition();
    }

    switch (this) {
      case PROGRAM:
        return loggingContext.getLogPartition();
      case APPLICATION:
        return namespaceId + ":" +
                loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_APPLICATION_ID).getValue();
      default:
        // this should never happen
        throw new IllegalArgumentException(
                String.format("Invalid log partition type %s. Allowed partition types are program/application",
                              getClass()));
    }
  }
}
