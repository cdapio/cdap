package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

/**
 *
 */
public class MDSJoinStage extends AbstractStage<VerificationStage.Input> {
  private final MetaDataStore mds;

  @Inject
  public MDSJoinStage(MetaDataStore mds) {
    super(TypeToken.of(VerificationStage.Input.class));
    this.mds = mds;
  }

  @Override
  public void process(final VerificationStage.Input o) throws Exception {

  }

}
