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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;


import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LauncherRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LauncherRunner.class);

  public void runnerMethod() throws Exception {

    YarnConfiguration conf = new YarnConfiguration();
    LocationFactory locationFactory = new FileContextLocationFactory(conf);
    TwillRunnerService twillRunner = new YarnTwillRunnerService(conf, "localhost:2181", locationFactory);
    twillRunner.start();

    try {
      TwillController controller = twillRunner.prepare(new TestTwillRunnable())
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
        .start();

      LOG.info("Job submitted");
      controller.awaitTerminated();
      LOG.info("Job completed");
    } finally {
      twillRunner.stop();
    }
  }

  /**
   *
   */
  public static final class TestTwillRunnable extends AbstractTwillRunnable {

    public void run() {
      LOG.info("Run called");
      try {
        TimeUnit.MINUTES.sleep(2);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted", e);
      }
      LOG.info("Run completed");
    }
  }
}
