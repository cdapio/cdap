package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

/**
 *
 */
public class MDSJoinStage extends AbstractStage<ApplicationSpecLocation> {
  private final MetaDataStore mds;

  @Inject
  public MDSJoinStage(MetaDataStore mds) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.mds = mds;
  }

  @Override
  public void process(final ApplicationSpecLocation o) throws Exception {

  }

}
