/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Tether runtime job manager. This class is sending runtime job details to the tethered CDAP instance through TMS.
 * An instance of this class is created by {@code TetherProvisioner}.
 */
public class TetherRuntimeJobManager implements RuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(TetherRuntimeJobManager.class);

  private final String tetheredInstanceName;
  private final String tetheredNamespace;

  public TetherRuntimeJobManager(String tetheredInstanceName, String tetheredNamespace) {
    this.tetheredInstanceName = tetheredInstanceName;
    this.tetheredNamespace = tetheredNamespace;
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    LOG.debug("Launching run {} with following configurations: tethered instance name {}, tethered namespace {}.",
              runInfo.getRun(), tetheredInstanceName, tetheredNamespace);
    // TODO: send RUN_PIPELINE message on TMS
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception {
    // TODO: get job details from tethered instance
    return Optional.empty();
  }

  @Override
  public List<RuntimeJobDetail> list() throws Exception {
    // TODO: get job details for all running jobs in tethered instance
    return null;
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    RuntimeJobDetail jobDetail = getDetail(programRunInfo).orElse(null);
    if (jobDetail == null) {
      return;
    }
    RuntimeJobStatus status = jobDetail.getStatus();
    if (status.isTerminated() || status == RuntimeJobStatus.STOPPING) {
      return;
    }
    // TODO: stop running job in tethered instance
  }

  @Override
  public void kill(ProgramRunInfo programRunInfo) throws Exception {
    stop(programRunInfo);
  }

  @Override
  public void close() {
    // no-op
  }
}
