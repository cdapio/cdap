/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import com.google.common.base.Strings;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.cdap.cdap.datapipeline.draft.DraftStore.TABLE_SPEC;

/**
 * Class to interact with the DraftStore
 */
public class DraftService {
  private static final Logger LOG = LoggerFactory.getLogger(DraftService.class);
  private final Metrics metrics;
  private final DraftStore store;

  public DraftService(TransactionRunner context, @Nullable Metrics metrics) {
    this.store = new DraftStore(context);
    this.metrics = metrics;
    if (metrics == null) {
      LOG.warn("Metrics collector was not injected into DraftHandler");
    }
  }

  /**
   * Returns a sorted and filtered list of drafts for the given namespace and owner
   *
   * @param namespaceSummary the namespace to fetch the drafts from
   * @param owner the id of the owner of the drafts
   * @param includeConfig the returned Draft objects will not include the pipeline config if this is false
   * @param sortRequest The sorting that should be applied to the results, pass null if no sorting is required. Note
   *   that the sortField matches against the column names in the table spec found in {@link DraftStore}
   * @param filter string used to do case-insensitive prefix matching on the draft name
   * @return List of {@link Draft}
   * @throws RuntimeException when an error occurs while fetching from the table
   */
  public List<Draft> listDrafts(NamespaceSummary namespaceSummary, String owner,
                                boolean includeConfig, SortRequest sortRequest,
                                @Nullable String filter) {
    List<Draft> drafts = store.listDrafts(namespaceSummary, owner, sortRequest, includeConfig);
    if (!Strings.isNullOrEmpty(filter)) {
      drafts = drafts.stream()
        .filter(draft -> draft.getName().toLowerCase().startsWith(filter.toLowerCase()))
        .collect(Collectors.toList());
    }
    return drafts;
  }

  /**
   * Fetch the given draft
   *
   * @param draftId {@link DraftId} that is used to uniquely identify a draft
   * @return the {@link Draft} object
   * @throws RuntimeException when an error occurs while fetching from the table
   * @throws DraftNotFoundException if the draft does not exist
   */
  public Draft getDraft(DraftId draftId) throws RuntimeException, DraftNotFoundException {
    return store.getDraft(draftId).orElseThrow(() -> new DraftNotFoundException(draftId));
  }

  /**
   * Write the given draft
   *
   * @param draftId {@link DraftId} that is used to uniquely identify a draft
   * @param draftStoreRequest {@link DraftStoreRequest} that contains the rest of the draft data
   * @throws RuntimeException when an error occurs while writing to the table
   */
  public <T extends ETLConfig> void writeDraft(DraftId draftId,
                                               DraftStoreRequest<T> draftStoreRequest) {
    // TODO(CDAP-17456): Add collision detection using the hashes
    store.writeDraft(draftId, draftStoreRequest);

    metrics.gauge(Constants.Metrics.DRAFT_COUNT, store.getDraftCount());
  }

  /**
   * Delete the given draft
   *
   * @param draftId {@link DraftId} that is used to uniquely identify a draft
   * @throws RuntimeException when an error occurs while fetching from the table
   * @throws DraftNotFoundException if the draft does not exist
   */
  public void deleteDraft(DraftId draftId) {
    // Make sure the draft exists before attempting to delete it
    getDraft(draftId);
    store.deleteDraft(draftId);

    metrics.gauge(Constants.Metrics.DRAFT_COUNT, store.getDraftCount());
  }

  /**
   * Checks if the given field exists in the {@link DraftStore} table spec. This should be used for validation.
   *
   * @param fieldName name of the field to check
   * @return True if the field exists in the {@link DraftStore} table spec
   */
  public boolean fieldExists(String fieldName) {
    return TABLE_SPEC.getFieldTypes().stream().anyMatch(f -> f.getName().equals(fieldName));
  }
}
