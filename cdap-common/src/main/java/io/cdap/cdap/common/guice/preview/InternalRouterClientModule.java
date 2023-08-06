package io.cdap.cdap.common.guice.preview;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.InternalRouter;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;

public final class InternalRouterClientModule extends AbstractModule {

  private final boolean useInternalRouter;

  public InternalRouterClientModule(CConfiguration cConf) {
    this.useInternalRouter = cConf.getBoolean(InternalRouter.USE_INTERNAL_ROUTER, false);
  }

  @Override
  protected void configure() {
    bindConstant().annotatedWith(
        Names.named(RemoteClientFactory.USE_INTERNAL_ROUTER)).to(useInternalRouter);
  }
}
