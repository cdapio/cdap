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

package io.cdap.cdap.app.runtime.spark;

/**
 * Constants used by spark extension are all defined here.
 */
public class Constant {
  /**
   * Spark on k8s
   */
  public static final class Spark {
    public static final class ArtifactFetcher {
      public static final String PORT = "artifact.fetcher.bind.port";
    }

    public static final class Driver {
      public static final String EXEC_THREADS = "driver.artifact.fetcher.exec.threads";
      public static final String BOSS_THREADS = "driver.artifact.fetcher.boss.threads";
      public static final String WORKER_THREADS = "driver.artifact.fetcher.worker.threads";
    }
  }
}
