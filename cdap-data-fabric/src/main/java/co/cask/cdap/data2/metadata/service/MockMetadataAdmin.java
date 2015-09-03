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
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link MetadataAdmin}
 */
public class MockMetadataAdmin implements MetadataAdmin {
  private static final String NOT_FOUND = "notfound";
  private static final String EMPTY = "empty";
  private static final Logger LOG = LoggerFactory.getLogger(MockMetadataAdmin.class);

  @Override
  public void add(Id.Application appId, Map<String, String> metadata)
    throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    LOG.info("Metadata for app '{}' added successfully.", appId);
  }

  @Override
  public void add(Id.Program programId, Map<String, String> metadata)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    LOG.info("Metadata for program '{}' added successfully.", programId);
  }

  @Override
  public void add(Id.DatasetInstance datasetId, Map<String, String> metadata)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    LOG.info("Metadata for dataset '{}' added successfully.", datasetId);
  }

  @Override
  public void add(Id.Stream streamId, Map<String, String> metadata)
    throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    LOG.info("Metadata for stream '{}' added successfully.", streamId);
  }

  @Override
  public Map<String, String> get(Id.Application appId) throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    Map<String, String> metadata;
    if (EMPTY.equals(appId.getNamespaceId()) || EMPTY.equals(appId.getId())) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("aKey", "aValue",
                                 "aK", "aV",
                                 "aK1", "aV1",
                                 "tags", "counter,mr,visual");
    }
    return metadata;
  }

  @Override
  public Map<String, String> get(Id.Program programId)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    Map<String, String> metadata;
    if (EMPTY.equals(programId.getNamespaceId()) || EMPTY.equals(programId.getApplicationId()) ||
      EMPTY.equals(programId.getId())) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("aKey", "aValue",
                                 "aK", "aV",
                                 "aK1", "aV1",
                                 "tags", "counter,mr,visual");
    }
    return metadata;
  }

  @Override
  public Map<String, String> get(Id.DatasetInstance datasetId)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    Map<String, String> metadata;
    if (EMPTY.equals(datasetId.getNamespaceId()) || EMPTY.equals(datasetId.getId())) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("dKey", "dValue",
                                 "dK", "dV",
                                 "dK1", "dV1",
                                 "tags", "reports,deviations,errors");
    }
    return metadata;
  }

  @Override
  public Map<String, String> get(Id.Stream streamId) throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    Map<String, String> metadata;
    if (EMPTY.equals(streamId.getNamespaceId()) || EMPTY.equals(streamId.getId())) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("sKey", "sValue",
                                 "sK", "sV",
                                 "sK1", "sV1",
                                 "tags", "input,raw");
    }
    return metadata;
  }

  @Override
  public void remove(Id.Application appId, String ... keys)
    throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    LOG.info("Metadata for app '{}' added successfully.", appId);
  }

  @Override
  public void remove(Id.Program programId, String ... keys)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    LOG.info("Metadata for program '{}' removed successfully.", programId);
  }

  @Override
  public void remove(Id.DatasetInstance datasetId, String ... keys)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    LOG.info("Metadata for dataset '{}' removed successfully.", datasetId);
  }

  @Override
  public void remove(Id.Stream streamId, String ... keys) throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    LOG.info("Metadata for stream '{}' removed successfully.", streamId);
  }

  @Override
  public void addTags(Id.Application appId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    LOG.info("Tags {} for app '{}' added successfully.", tags, appId);
  }

  @Override
  public void addTags(Id.Program programId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    LOG.info("Tags {} for program '{}' added successfully.", tags, programId);
  }

  @Override
  public void addTags(Id.DatasetInstance datasetId, String... tags)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    LOG.info("Tags {} for dataset '{}' added successfully.", tags, datasetId);
  }

  @Override
  public void addTags(Id.Stream streamId, String... tags) throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    LOG.info("Tags {} for stream '{}' added successfully.", tags, streamId);
  }

  @Override
  public Iterable<String> getTags(Id.Application appId)
    throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    List<String> tags = new ArrayList<>();
    tags.add("aTag");
    tags.add("aTag1");
    return tags;
  }

  @Override
  public Iterable<String> getTags(Id.Program programId)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    List<String> tags = new ArrayList<>();
    tags.add("pTag");
    tags.add("pTag10");
    return tags;
  }

  @Override
  public Iterable<String> getTags(Id.DatasetInstance datasetId)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    List<String> tags = new ArrayList<>();
    tags.add("dTag");
    tags.add("dTag1");
    return tags;
  }

  @Override
  public Iterable<String> getTags(Id.Stream streamId) throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    List<String> tags = new ArrayList<>();
    tags.add("sTag");
    tags.add("sTag10");
    return tags;
  }

  @Override
  public void removeTags(Id.Application appId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyApp(appId);
    LOG.info("Tags {} removed from app {}.", tags, appId);
  }

  @Override
  public void removeTags(Id.Program programId, String... tags)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyProgram(programId);
    LOG.info("Tags {} removed from program {}.", tags, programId);
  }

  @Override
  public void removeTags(Id.DatasetInstance datasetId, String... tags)
    throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyDataset(datasetId);
    LOG.info("Tags {} removed from dataset {}.", tags, datasetId);
  }

  @Override
  public void removeTags(Id.Stream streamId, String... tags)
    throws NamespaceNotFoundException, StreamNotFoundException {
    verifyStream(streamId);
    LOG.info("Tags {} removed from stream {}.", tags, streamId);
  }

  private void verifyNamespace(Id.Namespace namespaceId) throws NamespaceNotFoundException {
    if (NOT_FOUND.equals(namespaceId.getId())) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  private void verifyApp(Id.Application appId) throws NamespaceNotFoundException, ApplicationNotFoundException {
    verifyNamespace(appId.getNamespace());
    if (NOT_FOUND.equals(appId.getId())) {
      throw new ApplicationNotFoundException(appId);
    }
  }

  private void verifyProgram(Id.Program programId)
    throws NamespaceNotFoundException, ApplicationNotFoundException, ProgramNotFoundException {
    verifyApp(programId.getApplication());
    if (NOT_FOUND.equals(programId.getId())) {
      throw new ProgramNotFoundException(programId);
    }
  }

  private void verifyDataset(Id.DatasetInstance datasetId) throws NamespaceNotFoundException, DatasetNotFoundException {
    verifyNamespace(datasetId.getNamespace());
    if (NOT_FOUND.equals(datasetId.getId())) {
      throw new DatasetNotFoundException(datasetId);
    }
  }

  private void verifyStream(Id.Stream streamId) throws NamespaceNotFoundException, StreamNotFoundException {
    verifyNamespace(streamId.getNamespace());
    if (NOT_FOUND.equals(streamId.getId())) {
      throw new StreamNotFoundException(streamId);
    }
  }
}
