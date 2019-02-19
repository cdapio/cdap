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
import co.cask.cdap.common.metadata.MetadataRecord;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A metadata storage that delegates to another storage implementation
 * and publishes all metadata changes to the audit.
 */
public class AuditMetadataStorage implements MetadataStorage {

  private final MetadataStorage storage;
  private AuditPublisher auditPublisher;

  @Inject
  public AuditMetadataStorage(MetadataStorage storage) {
    this.storage = storage;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void createIndex() throws IOException {
    storage.createIndex();
  }

  @Override
  public void dropIndex() throws IOException {
    storage.dropIndex();
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation) throws IOException {
    MetadataChange change = storage.apply(mutation);
    publishAudit(change);
    return change;
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations) throws IOException {
    List<MetadataChange> changes = storage.batch(mutations);
    for (MetadataChange change : changes) {
      publishAudit(change);
    }
    return changes;
  }

  @Override
  public Metadata read(Read read) throws IOException {
    return storage.read(read);
  }

  @Override
  public SearchResponse search(SearchRequest request) throws IOException {
    return storage.search(request);
  }

  @Override
  public void close() {
    storage.close();
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
