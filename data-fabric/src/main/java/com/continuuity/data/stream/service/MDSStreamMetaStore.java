/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
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
