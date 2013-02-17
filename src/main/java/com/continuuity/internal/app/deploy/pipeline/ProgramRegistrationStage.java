package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;

/**
 *
 */
public class ProgramRegistrationStage extends AbstractStage<String> {

  public ProgramRegistrationStage() {
    super(TypeToken.of(String.class));
  }

  @Override
  public void process(final String o) throws Exception {

  }
}
