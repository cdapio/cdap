/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.common.test;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link Admin} that performs no-operation on all methods.
 */
public class NoopAdmin implements Admin {

  @Override
  public boolean datasetExists(String name) throws DatasetManagementException {
    return false;
  }

  @Override
  public String getDatasetType(String name) throws DatasetManagementException {
    throw new InstanceNotFoundException(name);
  }

  @Override
  public DatasetProperties getDatasetProperties(String name) throws DatasetManagementException {
    throw new InstanceNotFoundException(name);
  }

  @Override
  public void createDataset(String name, String type,
                            DatasetProperties properties) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void updateDataset(String name, DatasetProperties properties) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void dropDataset(String name) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void truncateDataset(String name) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
                  Map<String, String> properties) throws Exception {
    // no-op
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    // no-op
  }

  @Override
  public void createTopic(String topic) throws TopicAlreadyExistsException, IOException {
    // no-op
  }

  @Override
  public void createTopic(String topic,
                          Map<String, String> properties) throws TopicAlreadyExistsException, IOException {
    // no-op
  }

  @Override
  public Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException {
    return Collections.emptyMap();
  }

  @Override
  public void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException {
    // no-op
  }

  @Override
  public void deleteTopic(String topic) throws TopicNotFoundException, IOException {
    // no-op
  }

  @Override
  public boolean namespaceExists(String namespace) throws IOException {
    return false;
  }

  @Nullable
  @Override
  public NamespaceSummary getNamespaceSummary(String namespace) throws IOException {
    return null;
  }
}
