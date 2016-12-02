/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.app.runtime.AbstractProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.proto.InMemoryProgramLiveInfo;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link ProgramRuntimeService} for in memory mode. It is used for unit-test as well as in Standalone.
 */
public final class InMemoryProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryProgramRuntimeService.class);

  private final String hostname;

  @Inject
  public InMemoryProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, CConfiguration cConf,
                                       ArtifactRepository artifactRepository,
                                       @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
    super(cConf, programRunnerFactory, artifactRepository);
    this.hostname = hostname.getCanonicalHostName();
  }

  @Override
  public ProgramLiveInfo getLiveInfo(ProgramId programId) {
    return isRunning(programId) ? new InMemoryProgramLiveInfo(programId)
      : new NotRunningProgramLiveInfo(programId);
  }

  @Override
  protected Map<String, String> getExtraProgramOptions() {
    return Collections.singletonMap(ProgramOptionConstants.HOST, hostname);
  }

  @Override
  protected void shutDown() throws Exception {
    stopAllPrograms();
  }

  private void stopAllPrograms() {

    LOG.info("Stopping all running programs.");

    List<ListenableFuture<ProgramController>> futures = Lists.newLinkedList();
    for (ProgramType type : ProgramType.values()) {
      for (Map.Entry<RunId, RuntimeInfo> entry : list(type).entrySet()) {
        RuntimeInfo runtimeInfo = entry.getValue();
        if (isRunning(runtimeInfo.getProgramId())) {
          futures.add(runtimeInfo.getController().stop());
        }
      }
    }
    // unchecked because we cannot do much if it fails. We will still shutdown the standalone CDAP instance.
    try {
      Futures.successfulAsList(futures).get(60, TimeUnit.SECONDS);
      LOG.info("All programs have been stopped.");
    } catch (ExecutionException e) {
      // note this should not happen because we wait on a successfulAsList
      LOG.warn("Got exception while waiting for all programs to stop", e.getCause());
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception while waiting for all programs to stop", e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      // can't do much more than log it. We still want to exit.
      LOG.warn("Timeout while waiting for all programs to stop.");
    }
  }
}
