/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.webapp;

import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.http.NettyHttpService;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controller for webapp.
 */
public class WebappProgramController extends AbstractProgramController {
  private static final Logger LOG = LoggerFactory.getLogger(WebappProgramController.class);

  private final NettyHttpService httpService;
  private final Cancellable cancellable;

  public WebappProgramController(String programName, RunId runId, NettyHttpService httpService,
                                 Cancellable cancellable) {
    super(programName, runId);
    this.httpService = httpService;
    this.cancellable = cancellable;
    started();
  }

  @Override
  protected void doSuspend() throws Exception {
    // no-op
  }

  @Override
  protected void doResume() throws Exception {
    // no-op
  }

  @Override
  protected void doStop() throws Exception {
    LOG.info("Stopping webapp...");
    cancellable.cancel();
    httpService.stopAndWait();
    LOG.info("Webapp stopped.");
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // no-op
  }
}
