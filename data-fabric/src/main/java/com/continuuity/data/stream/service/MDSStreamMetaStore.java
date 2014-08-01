/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.data.stream.service;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.metadata.MetaDataEntry;
import com.continuuity.metadata.MetaDataTable;
import com.google.gson.Gson;
import com.google.inject.Inject;

/**
 * Implementation of {@link StreamMetaStore} that access MDS directly.
 */
public final class MDSStreamMetaStore implements StreamMetaStore {

  private static final Gson GSON = new Gson();

  // Constatns copied from app-fabric
  public static final String ENTRY_TYPE = "str";
  public static final String SPEC_TYPE = "spec";

  private final MetaDataTable metaDataTable;

  @Inject
  MDSStreamMetaStore(MetaDataTable metaDataTable) {
    this.metaDataTable = metaDataTable;
  }


  @Override
  public void addStream(String accountId, String streamName) throws Exception {
    String json = GSON.toJson(createStreamSpec(streamName));
    OperationContext context = new OperationContext(accountId);
    MetaDataEntry existing = metaDataTable.get(context, accountId, null, ENTRY_TYPE, streamName);
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(accountId, null, ENTRY_TYPE, streamName);
      entry.addField(SPEC_TYPE, json);
      metaDataTable.add(context, entry);
    } else {
      metaDataTable.updateField(context, accountId, null, ENTRY_TYPE, streamName, SPEC_TYPE, json, -1);
    }
  }

  @Override
  public void removeStream(String accountId, String streamName) throws Exception {
    OperationContext context = new OperationContext(accountId);
    metaDataTable.delete(context, accountId, null, ENTRY_TYPE, streamName);
  }

  @Override
  public boolean streamExists(String accountId, String streamName) throws Exception {
    OperationContext context = new OperationContext(accountId);
    MetaDataEntry existing = metaDataTable.get(context, accountId, null, ENTRY_TYPE, streamName);
    return existing != null;
  }

  private StreamSpecification createStreamSpec(String streamName) {
    return new StreamSpecification.Builder().setName(streamName).create();
  }
}
