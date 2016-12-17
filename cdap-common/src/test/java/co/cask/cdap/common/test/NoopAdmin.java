/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.test;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    // no-op
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
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
}
