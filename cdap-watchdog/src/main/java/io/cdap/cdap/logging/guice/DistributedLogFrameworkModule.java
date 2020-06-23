/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.logging.framework.CustomLogPipelineConfigProvider;
import io.cdap.cdap.logging.framework.distributed.DistributedAppenderContext;
import io.cdap.cdap.logging.framework.distributed.DistributedLogFramework;
import io.cdap.cdap.logging.service.LogSaverStatusService;
import io.cdap.http.HttpHandler;

import java.io.File;
import java.util.List;

/**
 * Guice module for the distributed log framework.
 */
public class DistributedLogFrameworkModule extends PrivateModule {

  private final int instanceId;
  private final int instanceCount;

  public DistributedLogFrameworkModule(int instanceId, int instanceCount) {
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
  }

  @Override
  protected void configure() {
    bind(Integer.class).annotatedWith(Names.named(Constants.LogSaver.LOG_SAVER_INSTANCE_ID)).toInstance(instanceId);
    bind(Integer.class).annotatedWith(Names.named(Constants.LogSaver.LOG_SAVER_INSTANCE_COUNT))
      .toInstance(instanceCount);
    bind(AppenderContext.class).to(DistributedAppenderContext.class);

    // Bind the status service
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder
      (binder(), HttpHandler.class, Names.named(Constants.LogSaver.LOG_SAVER_STATUS_HANDLER));
    CommonHandlers.add(handlerBinder);
    bind(LogSaverStatusService.class).in(Scopes.SINGLETON);
    expose(LogSaverStatusService.class);

    bind(DistributedLogFramework.class).in(Scopes.SINGLETON);
    expose(DistributedLogFramework.class);
  }

  @Provides
  public CustomLogPipelineConfigProvider provideCustomLogConfig(CConfiguration cConf) {
    return () -> DirUtils.listFiles(new File(cConf.get(Constants.Logging.PIPELINE_CONFIG_DIR)), "xml");
  }
}
