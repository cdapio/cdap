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

package io.cdap.cdap.master.spi.environment;

/**
 * A executable that can be launched by {@link MasterEnvironmentContext#getRunnableArguments(Class, String...)}
 * as a separate process.
 *
 * Implementation class should have a constructor that takes two arguments in the following order
 *
 * {@link MasterEnvironmentContext} and {@link MasterEnvironment}
 */
public interface MasterEnvironmentRunnable {

  /**
   * This act like a Java main method, which will be called after the {@link MasterEnvironment} was initialize.
   * This method should block until it finishes its task.
   */
  void run(String[] args) throws Exception;

  /**
   * Invokes to signal interruption to the {@link #run(String[])} to return.
   */
  void stop();
}
