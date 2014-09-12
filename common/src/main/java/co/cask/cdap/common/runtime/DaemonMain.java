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
package co.cask.cdap.common.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * A abstract base class for bridging standard main method to method invoked through
 * apache commons-daemon jsvc.
 */
public abstract class DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(DaemonMain.class);

  /**
   * The main method. It simply call methods in the same sequence
   * as if the program is started by jsvc.
   */
  protected void doMain(final String[] args) throws Exception {
    final CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          try {
            DaemonMain.this.stop();
          } finally {
            try {
              DaemonMain.this.destroy();
            } finally {
              shutdownLatch.countDown();
            }
          }
        } catch (Throwable t) {
          LOG.error("Exception when shutting down: " + t.getMessage(), t);
        }
      }
    });
    init(args);
    start();

    shutdownLatch.await();
  }

  /**
   * Invoked by jsvc to initialize the program.
   */
  public abstract void init(String[] args);

  /**
   * Invoked by jsvc to start the program.
   */
  public abstract void start();

  /**
   * Invoked by jsvc to stop the program.
   */
  public abstract void stop();

  /**
   * Invoked by jsvc for resource cleanup.
   */
  public abstract void destroy();
}
