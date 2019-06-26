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

package io.cdap.cdap.data2.metadata.writer;

import io.cdap.cdap.spi.metadata.MetadataMutation;

import java.util.List;

/**
 * This interface exposes functionality for making Metadata HTTP calls to the Metadata Service.
 */
public interface MetadataServiceClient {

  /**
   * Performs the create mutation via Metadata Service.
   *
   * @param createMutation Metadata's create mutation to apply
   */
  void create(MetadataMutation.Create createMutation);

  /**
   * Performs the drop mutation via Metadata Service.
   *
   * @param dropMutation Metadata's drop mutation to apply
   */
  void drop(MetadataMutation.Drop dropMutation);

  /**
   * Performs the update mutation via Metadata Service.
   *
   * @param updateMutation Metadata's update mutation to apply
   */
  void update(MetadataMutation.Update updateMutation);

  /**
   * Performs the remove mutation via Metadata Service.
   *
   * @param removeMutation Metadata's remove mutation to apply
   */
  void remove(MetadataMutation.Remove removeMutation);

  /**
   * Performs a batch of metadata mutation via Metadata Service.
   *
   * @param mutations a list of metadata mutations to apply
   */
  void batch(List<MetadataMutation> mutations);
}
