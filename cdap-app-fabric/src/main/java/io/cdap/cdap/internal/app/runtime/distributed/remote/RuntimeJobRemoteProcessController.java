/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * A {@link RemoteProcessController} implmentation using {@link RuntimeJobManager}.
 */
class RuntimeJobRemoteProcessController implements RemoteProcessController {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeJobRemoteProcessController.class);

  private final Supplier<RuntimeJobManager> runtimeJobManagerSupplier;
  private final ProgramRunId programRunId;
  private final ProgramRunInfo programRunInfo;

  RuntimeJobRemoteProcessController(ProgramRunId programRunId, Supplier<RuntimeJobManager> runtimeJobManagerSupplier) {
    this.runtimeJobManagerSupplier = runtimeJobManagerSupplier;
    this.programRunId = programRunId;
    this.programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace(programRunId.getNamespace())
      .setApplication(programRunId.getApplication())
      .setVersion(programRunId.getVersion())
      .setProgramType(programRunId.getType().name())
      .setProgram(programRunId.getProgram())
      .setRun(programRunId.getRun())
      .build();
  }

  @Override
  public boolean isRunning() throws Exception {
    try (RuntimeJobManager runtimeJobManager = runtimeJobManagerSupplier.get()) {
      return !runtimeJobManager.getDetail(programRunInfo)
        .map(RuntimeJobDetail::getStatus)
        .map(RuntimeJobStatus::isTerminated)
        .orElse(true);
    }
  }

  @Override
  public void terminate() throws Exception {
    LOG.debug("Stopping program run {}", programRunId);
    try (RuntimeJobManager runtimeJobManager = runtimeJobManagerSupplier.get()) {
      runtimeJobManager.stop(programRunInfo);
    }
  }

  @Override
  public void kill() throws Exception {
    LOG.debug("Force stopping program run {}", programRunId);
    try (RuntimeJobManager runtimeJobManager = runtimeJobManagerSupplier.get()) {
      runtimeJobManager.kill(programRunInfo);
    }
  }
}
