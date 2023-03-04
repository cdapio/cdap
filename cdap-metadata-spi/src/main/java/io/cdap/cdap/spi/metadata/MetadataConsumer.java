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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Interface for consuming programs and lineage metadata.
 */
@Beta
public interface MetadataConsumer {

  /**
   * @return the name of the metadata consumer.
   */
  String getName();

  /**
   * Consumes lineage info for a program run.
   *
   * @param context context for the MetadataConsumer containing properties required by the
   *     implementation.
   * @param programRun the {@link ProgramRun} for which lineage is consumed
   * @param lineageInfo the {@link LineageInfo} containing details of lineage for the program
   *     run
   * @throws Exception if there is any error while consuming lineage.
   */
  void consumeLineage(MetadataConsumerContext context, ProgramRun programRun,
      LineageInfo lineageInfo)
      throws Exception;
}

