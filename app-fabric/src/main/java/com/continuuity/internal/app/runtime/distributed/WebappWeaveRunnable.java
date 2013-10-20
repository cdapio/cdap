/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.google.inject.Module;
import com.google.inject.util.Modules;

/**
 * Weave runnable wrapper for webapp.
 */
final class WebappWeaveRunnable extends AbstractProgramWeaveRunnable<WebappProgramRunner> {

  WebappWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WebappProgramRunner> getProgramClass() {
    return WebappProgramRunner.class;
  }

  @Override
  protected Module createModule(WeaveContext context, ZKClientService zkClientService,
                                KafkaClientService kafkaClientService) {
    return Modules.combine(super.createModule(context, zkClientService, kafkaClientService),
                           new DiscoveryRuntimeModule(zkClientService).getDistributedModules());
  }
}
