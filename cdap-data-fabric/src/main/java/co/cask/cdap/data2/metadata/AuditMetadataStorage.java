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

package co.cask.cdap.data2.metadata;

import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsCollector;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataChange;
import co.cask.cdap.spi.metadata.MetadataMutation;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.Read;
import co.cask.cdap.spi.metadata.SearchRequest;
import co.cask.cdap.spi.metadata.SearchResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A metadata storage that delegates to another storage implementation
 * and publishes all metadata changes to the audit.
 */
public class AuditMetadataStorage implements MetadataStorage {
  private static final Map<MetadataMutation.Type, String> MUTATION_COUNT_MAP;
  private static final Map<MetadataMutation.Type, String> MUTATION_ERROR_MAP;
  static {
    ImmutableMap.Builder<MetadataMutation.Type, String> countBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<MetadataMutation.Type, String> errorBuilder = ImmutableMap.builder();
    EnumSet.allOf(MetadataMutation.Type.class).forEach(type -> {
      countBuilder.put(type, type.name().toLowerCase() + ".count");
      errorBuilder.put(type, type.name().toLowerCase() + ".error");
    });
    MUTATION_COUNT_MAP = countBuilder.build();
    MUTATION_ERROR_MAP = errorBuilder.build();
  }

  private final MetadataStorage storage;
  private final MetricsCollectionService metricsCollectionService;
  private AuditPublisher auditPublisher;

  @Inject
  public AuditMetadataStorage(@Named(DataSetsModules.SPI_BASE_IMPL) MetadataStorage storage,
                              MetricsCollectionService metricsCollectionService) {
    this.storage = storage;
    this.metricsCollectionService = metricsCollectionService;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void createIndex() throws IOException {
    try {
      storage.createIndex();
      emitMetrics("createIndex.count");
    } catch (Exception e) {
      emitMetrics("createIndex.error");
      throw e;
    }
  }

  @Override
  public void dropIndex() throws IOException {
    try {
      storage.dropIndex();
      emitMetrics("dropIndex.count");
    } catch (Exception e) {
      emitMetrics("dropIndex.error");
      throw e;
    }
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation) throws IOException {
    MetadataChange change;
    try {
      change = storage.apply(mutation);
      emitMetrics(MUTATION_COUNT_MAP.get(mutation.getType()));
    } catch (Exception e) {
      emitMetrics(MUTATION_ERROR_MAP.get(mutation.getType()));
      throw e;
    }
    publishAudit(change);
    return change;
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations) throws IOException {
    List<MetadataChange> changes;
    try {
      changes = storage.batch(mutations);
      for (MetadataMutation metadataMutation : mutations) {
        emitMetrics(MUTATION_COUNT_MAP.get(metadataMutation.getType()));
      }
    } catch (Exception e) {
      for (MetadataMutation metadataMutation : mutations) {
        emitMetrics(MUTATION_ERROR_MAP.get(metadataMutation.getType()));
      }
      throw e;
    }

    for (MetadataChange change : changes) {
      publishAudit(change);
    }
    return changes;
  }

  @Override
  public Metadata read(Read read) throws IOException {
    try {
      Metadata metadata = storage.read(read);
      emitMetrics("read.count");
      return metadata;
    } catch (Exception e) {
      emitMetrics("read.error");
      throw e;
    }
  }

  @Override
  public SearchResponse search(SearchRequest request) throws IOException {
    try {
      SearchResponse result = storage.search(request);
      emitMetrics("search.count");
      return result;
    } catch (Exception e) {
      emitMetrics("search.error");
      throw e;
    }
  }

  @Override
  public void close() {
    storage.close();
  }

  private void emitMetrics(String metricSuffix) {
    MetricsCollector metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
    metricsCollector.increment(Constants.Metrics.MetadataStorage.METRICS_PREFIX + metricSuffix, 1L);
  }

  private void publishAudit(MetadataChange change) {
    publishAudit(change, MetadataScope.SYSTEM);
    publishAudit(change, MetadataScope.USER);
  }

  private void publishAudit(MetadataChange change, MetadataScope scope) {
    Map<String, String> propsBefore = change.getBefore().getProperties(scope);
    Map<String, String> propsAfter = change.getAfter().getProperties(scope);
    Set<String> tagsBefore = change.getBefore().getTags(scope);
    Set<String> tagsAfter = change.getAfter().getTags(scope);

    boolean propsChanged = !propsBefore.equals(propsAfter);
    boolean tagsChanged = !tagsBefore.equals(tagsAfter);
    if (!propsChanged && !tagsChanged) {
      return; // no change to log
    }

    // previous state is already given
    MetadataRecord previous = new MetadataRecord(change.getEntity(), scope, propsBefore, tagsBefore);

    // compute what was added
    @SuppressWarnings("ConstantConditions")
    Map<String, String> propsAdded = Maps.filterEntries(
      propsAfter, entry -> !entry.getValue().equals(propsBefore.get(entry.getKey())));
    Set<String> tagsAdded = Sets.difference(tagsAfter, tagsBefore);
    MetadataRecord additions = new MetadataRecord(change.getEntity(), scope, propsAdded, tagsAdded);

    // compute what was deleted
    @SuppressWarnings("ConstantConditions")
    Map<String, String> propsDeleted = Maps.filterEntries(
      propsBefore, entry -> !entry.getValue().equals(propsAfter.get(entry.getKey())));
    Set<String> tagsDeleted = Sets.difference(tagsBefore, tagsAfter);
    MetadataRecord deletions = new MetadataRecord(change.getEntity(), scope, propsDeleted, tagsDeleted);

    // and publish
    MetadataPayload payload = new MetadataPayloadBuilder()
      .addPrevious(previous)
      .addAdditions(additions)
      .addDeletions(deletions)
      .build();
    AuditPublishers.publishAudit(auditPublisher, previous.getMetadataEntity(), AuditType.METADATA_CHANGE, payload);
  }
}
