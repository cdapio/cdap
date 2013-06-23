package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.app.store.Store;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;

/**
 *
 */
public class ApplicationRegistrationStage extends AbstractStage<ApplicationWithPrograms> {
  private final Store store;

  public ApplicationRegistrationStage(Store store) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.store = store;
  }

  @Override
  public void process(final ApplicationWithPrograms o) throws Exception {
    store.addApplication(o.getAppSpecLoc().getApplicationId(),
                         o.getAppSpecLoc().getSpecification(),
                         o.getAppSpecLoc().getArchive());
    emit(o);
  }
}
