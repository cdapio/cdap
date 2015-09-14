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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * Writes program-dataset access information along with metadata  into {@link LineageStore}.
 */
public class BasicLineageWriter implements LineageWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BasicLineageWriter.class);
  private static final Gson GSON = new Gson();

  private final BusinessMetadataStore businessMetadataStore;
  private final LineageStore lineageStore;

  private final ConcurrentMap<CheckKey, Boolean> registered = new ConcurrentHashMap<>();

  @Inject
  BasicLineageWriter(BusinessMetadataStore businessMetadataStore, LineageStore lineageStore) {
    this.businessMetadataStore = businessMetadataStore;
    this.lineageStore = lineageStore;
  }

  @Override
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType) {
    addAccess(run, datasetInstance, accessType, null);
  }

  @Override
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType,
                        @Nullable Id.NamespacedId component) {
    if (alreadyRegistered(run, datasetInstance, accessType, component)) {
      return;
    }

    String metadata = gatherMetadata(run, datasetInstance);
    LOG.debug("Writing access for run {}, dataset {}, accessType {}, component {}, metadata = {}",
              run, datasetInstance, accessType, component, metadata);
    lineageStore.addAccess(run, datasetInstance, accessType, metadata, component);
  }

  @Override
  public void addAccess(Id.Run run, Id.Stream stream, AccessType accessType) {
    addAccess(run, stream, accessType, null);
  }

  @Override
  public void addAccess(Id.Run run, Id.Stream stream, AccessType accessType, @Nullable Id.NamespacedId component) {
    if (alreadyRegistered(run, stream, accessType, component)) {
      return;
    }

    String metadata = gatherMetadata(run, stream);
    LOG.debug("Writing access for run {}, stream {}, accessType {}, component {}, metadata = {}",
              run, stream, accessType, component, metadata);
    lineageStore.addAccess(run, stream, accessType, metadata, component);
  }

  private String gatherMetadata(Id.Run run, Id.NamespacedId id) {
    // TODO: add app metadata and tags
    Map<String, String> dataMetadata = businessMetadataStore.getProperties(id);
    return GSON.toJson(dataMetadata);
  }

  private boolean alreadyRegistered(Id.Run run, Id.NamespacedId data, AccessType accessType,
                                    @Nullable Id.NamespacedId component) {
    return registered.putIfAbsent(new CheckKey(run, data, accessType, component), true) != null;
  }

  private static final class CheckKey {
    private final Id.Run run;
    private final Id.NamespacedId data;
    private final AccessType accessType;
    private final Id.NamespacedId component;

    public CheckKey(Id.Run run, Id.NamespacedId data, AccessType accessType, @Nullable Id.NamespacedId component) {
      this.run = run;
      this.data = data;
      this.accessType = accessType;
      this.component = component;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CheckKey)) {
        return false;
      }
      CheckKey checkKey = (CheckKey) o;
      return Objects.equals(run, checkKey.run) &&
        Objects.equals(data, checkKey.data) &&
        Objects.equals(accessType, checkKey.accessType) &&
        Objects.equals(component, checkKey.component);
    }

    @Override
    public int hashCode() {
      return Objects.hash(run, data, accessType, component);
    }
  }
}
