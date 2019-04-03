/*
 * Copyright © 2019 Cask Data, Inc.
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
 * Represents a periodic task.
 */
public interface MasterEnvironmentTask {

  /**
   * Called by the system to run a short living task.
   * If {@link RuntimeException} is raised from this method, the exception will be logged and
   * the {@link #failureRetryDelay(Throwable)} will be called to determined the delay in milliseconds for
   * the next call to this method.
   *
   * @param context a {@link MasterEnvironmentContext} to provide information about the CDAP environment
   * @return delay in milliseconds till the next time this method will be invoked. If it returns a negative number,
   *         no more invocation will happen
   */
  long run(MasterEnvironmentContext context);

  /**
   * Returns the delay in milliseconds till the next time the {@link #run(MasterEnvironmentContext)} gets invoked
   * again when there was an exception raised by the {@link #run(MasterEnvironmentContext)} method.
   *
   * @param t the exception raised by the {@link #run(MasterEnvironmentContext)} method
   * @return delay in milliseconds. By default is 5000 milliseconds.
   */
  default long failureRetryDelay(Throwable t) {
    return 5000L;
  }
}
