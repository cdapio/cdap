/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.app.deploy.ProgramRunDispatcher;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryProgramRunDispatcher;
import io.cdap.cdap.internal.app.deploy.pipeline.ProgramRunDispatcherInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.apache.twill.api.RunId;

import java.util.concurrent.ExecutorService;

/**
 * Implementation of {@link RunnableTask} to execute Program-run operation in a system worker.
 */
public class ProgramRunDispatcherTask implements RunnableTask {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
    .registerTypeAdapter(Requirements.class, new RequirementsCodec())
    .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private final Injector injector;
  private final ExecutorService executorService;

  @Inject
  ProgramRunDispatcherTask(Injector injector,
                           @Named(Constants.SystemWorker.CLEANUP_EXECUTOR_SERVICE_BINDING)
                             ExecutorService executorService) {
    this.injector = injector;
    this.executorService = executorService;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    ProgramRunDispatcherInfo programRunDispatcherInfo =
      GSON.fromJson(context.getParam(), ProgramRunDispatcherInfo.class);
    ProgramRunDispatcher dispatcher = injector.getInstance(InMemoryProgramRunDispatcher.class);
    ProgramController programController = dispatcher.dispatchProgram(programRunDispatcherInfo);
    if (programController == null) {
      String msg = String.format("Unable to dispatch Program %s with runid %s",
                                 programRunDispatcherInfo.getProgramDescriptor().getProgramId(),
                                 programRunDispatcherInfo.getRunId());
      throw Throwables.propagate(new Throwable(msg));
    }
    // Result doesn't matter since we just need an HTTP 200 response or an exception in case of an error(handled above).
    context.writeResult(new byte[0]);
    programController.stop();
    executorService.submit(programRunDispatcherInfo.getCleanUpTask().get());
  }
}
