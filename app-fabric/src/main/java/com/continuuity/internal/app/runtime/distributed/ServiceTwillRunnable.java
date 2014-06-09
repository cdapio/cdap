package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.twillservice.ServiceRunnableProgramRunner;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.twill.api.TwillContext;

/**
 *
 */
public class ServiceTwillRunnable extends AbstractProgramTwillRunnable<ServiceRunnableProgramRunner> {

  ServiceTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<ServiceRunnableProgramRunner> getProgramClass() {
    return ServiceRunnableProgramRunner.class;
  }

  @Override
  protected Module createModule(final TwillContext context) {
    return Modules.combine(super.createModule(context),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(TwillContext.class).toInstance(context);
                             }
                           });
  }
}
