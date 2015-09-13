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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.proto.Id;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Writes program-dataset access information along with metadata  into {@link LineageStore}.
 */
public class BasicLineageWriter implements LineageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BasicLineageWriter.class);
  private static final Gson GSON = new Gson();

  private final BusinessMetadataStore businessMetadataStore;
  private final LineageStore lineageStore;

  @Inject
  public BasicLineageWriter(BusinessMetadataStore businessMetadataStore, LineageStore lineageStore) {
    this.businessMetadataStore = businessMetadataStore;
    this.lineageStore = lineageStore;
  }

  @Override
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType) {
    addAccess(run, datasetInstance, accessType, null);
  }

  @Override
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType,
                        Id.NamespacedId component) {
    // TODO: avoid duplicate writes for a dataset instance
    String metadata = gatherMetadata(run, datasetInstance);
    LOG.debug("Writing access for run {}, dataset {}, accessType {}, component {}, metadata = {}",
              run, datasetInstance, accessType, component, metadata);
    lineageStore.addAccess(run, datasetInstance, accessType, metadata, component);
  }

  private String gatherMetadata(Id.Run run, Id.DatasetInstance datasetInstance) {
    // TODO: add app metadata and tags
    Map<String, String> datasetMetadata = businessMetadataStore.getProperties(datasetInstance);
    return GSON.toJson(datasetMetadata);
  }
}
