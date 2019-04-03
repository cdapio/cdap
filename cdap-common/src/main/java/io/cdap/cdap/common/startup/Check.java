/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.startup;

/**
 * Checks something at start up, throwing an exception if the check failed. Run by a {@link CheckRunner}.
 * Checks are instantiated by the {@link CheckRunner} using an {@link com.google.inject.Injector} to give them access
 * to objects they may need, such as a LocationFactory or CConfiguration.
 */
public abstract class Check {

  /**
   * Run the check, throwing an Exception if the check failed.
   */
  public abstract void run() throws Exception;

  /**
   * Return a short, descriptive name for the check.
   *
   * @return the name of the check
   */
  public String getName() {
    return getClass().getSimpleName();
  }
}
