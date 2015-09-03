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

package co.cask.cdap.data2.metadata.service;

import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.proto.Id;

import java.util.Map;

/**
 * Interface to interact with Metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.Application}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void add(Id.Application appId, Map<String, String> metadata)
    throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.Program}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  void add(Id.Program programId, Map<String, String> metadata)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.DatasetInstance}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  void add(Id.DatasetInstance datasetId, Map<String, String> metadata)
    throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.Stream}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  void add(Id.Stream streamId, Map<String, String> metadata)
    throws NamespaceNotFoundException, StreamNotFoundException;

  /**
   * Adds the specified tags to specified {@link Id.Application}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void addTags(Id.Application appId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * Adds the specified tags to specified {@link Id.Program}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  void addTags(Id.Program programId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * Adds the specified tags to specified {@link Id.DatasetInstance}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  void addTags(Id.DatasetInstance datasetId, String... tags)
    throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * Adds the specified tags to specified {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  void addTags(Id.Stream streamId, String... tags)
    throws NamespaceNotFoundException, StreamNotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.Application}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  Map<String, String> get(Id.Application appId) throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.Program}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  Map<String, String> get(Id.Program appId)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.DatasetInstance}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  Map<String, String> get(Id.DatasetInstance datasetId) throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.Stream}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  Map<String, String> get(Id.Stream streamId) throws NamespaceNotFoundException, StreamNotFoundException;

  /**
   * @return all the tags for the specified {@link Id.Application}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  Iterable<String> getTags(Id.Application appId) throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * @return all the tags for the specified {@link Id.Program}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  Iterable<String> getTags(Id.Program programId)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * @return all the tags for the specified {@link Id.DatasetInstance}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  Iterable<String> getTags(Id.DatasetInstance datasetId) throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * @return all the tags for the specified {@link Id.Stream}.
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  Iterable<String> getTags(Id.Stream streamId) throws NamespaceNotFoundException, StreamNotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.Application}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void remove(Id.Application appId, String ... keys) throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.Program}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  void remove(Id.Program programId, String ... keys)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.DatasetInstance}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  void remove(Id.DatasetInstance datasetInstance, String ... keys)
    throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  void remove(Id.Stream streamId, String ... keys) throws NamespaceNotFoundException, StreamNotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.Application}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void removeTags(Id.Application appId, String ... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.Program}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application containing the program is not found
   * @throws ProgramNotFoundException if the program is not found
   */
  void removeTags(Id.Program programId, String ... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.DatasetInstance}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws DatasetNotFoundException if the dataset is not found
   */
  void removeTags(Id.DatasetInstance datasetId, String ... tags)
    throws NamespaceNotFoundException, DatasetNotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws StreamNotFoundException if the stream is not found
   */
  void removeTags(Id.Stream streamId, String ... tags) throws NamespaceNotFoundException, StreamNotFoundException;
}
