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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.app.deploy.ProgramRunDispatcher;
import io.cdap.cdap.app.deploy.ProgramRunDispatcherContext;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryProgramRunDispatcher;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;

/**
 * Implementation of {@link RunnableTask} to execute Program-run operation in a system worker.
 */
public class ProgramRunDispatcherTask implements RunnableTask {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .registerTypeAdapter(RunId.class, new RunIds.RunIdCodec())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();

  private final Injector injector;

  @Inject
  ProgramRunDispatcherTask(Injector injector) {
    this.injector = injector;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    // Need to copy the instance to restore transient fields
    ProgramRunDispatcherContext dispatcherContext = new ProgramRunDispatcherContext(
        GSON.fromJson(context.getParam(), ProgramRunDispatcherContext.class));
    context.setCleanupTask(dispatcherContext::executeCleanupTasks);
    ProgramRunId programRunId =
        dispatcherContext.getProgramDescriptor().getProgramId().run(dispatcherContext.getRunId());

    ProgramRunDispatcher dispatcher = injector.getInstance(InMemoryProgramRunDispatcher.class);
    ProgramController programController;
    try {
      programController = dispatcher.dispatchProgram(dispatcherContext);
    } catch (Exception e) {
      throw new Exception(String.format("Failed to dispatch program run %s", programRunId), e);
    }

    if (programController == null) {
      throw new IllegalStateException("Failed to dispatch program run " + programRunId);
    }
    // Result doesn't matter since we just need to return with 200 status or throw an exception
    context.writeResult(new byte[0]);
  }
}
