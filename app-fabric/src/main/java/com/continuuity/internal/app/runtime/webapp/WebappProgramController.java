package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.common.Cancellable;
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
