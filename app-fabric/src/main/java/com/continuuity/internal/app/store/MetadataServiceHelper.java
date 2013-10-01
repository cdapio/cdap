/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;


import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.Id;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.MetadataServiceException;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Stream;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hack hack hack: time constraints
 * This is needed for updating info in metadataService which we have to keep in sync with new stuff which came with
 * new App Fabric since old UI still relies on it.
 */
class MetadataServiceHelper {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceHelper.class);
  /**
   * We re-use metadataService to store configuration type data
   */
  private MetaDataStore metaDataService;

  public MetadataServiceHelper(MetaDataStore metaDataService) {
    this.metaDataService = metaDataService;
  }

  public void updateInMetadataService(final Id.Application id, final ApplicationSpecification spec) {
    String account = id.getAccountId();
    try {
      // datasets
      for (DataSetSpecification datasetSpec : spec.getDataSets().values()) {
        updateInMetadataService(account, datasetSpec);
      }

      // streams
      for (StreamSpecification streamSpec: spec.getStreams().values()) {
        updateInMetadataService(account, streamSpec);
      }

    } catch (MetadataServiceException e) {
      throw Throwables.propagate(e);
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }


  private void updateInMetadataService(final String account, final StreamSpecification streamSpec)
    throws MetadataServiceException, TException {
    Stream stream = new Stream(streamSpec.getName());
    stream.setName(streamSpec.getName());
    // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
    //       addApplication() and hence the call is successful
    metaDataService.assertStream(account, stream);
  }

  private void updateInMetadataService(final String account, final DataSetSpecification datasetSpec)
    throws MetadataServiceException, TException {
    Dataset dataset = new Dataset(datasetSpec.getName());
    // no description in datasetSpec
    dataset.setName(datasetSpec.getName());
    dataset.setDescription("");
    dataset.setType(datasetSpec.getType());
    dataset.setSpecification(new Gson().toJson(datasetSpec));
    // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
    //       addApplication() and hence the call is successful
    metaDataService.assertDataset(account, dataset);
  }

  public void deleteAll(Id.Account id) throws TException, MetadataServiceException {
    metaDataService.deleteAll(id.getId());
  }
}
