/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.service;

/**
 * Interface implemented by classes that provide ability to
 * stop resources or subsystems that are started.
 */
public interface Stoppable {

  /**
   * Closes any resources held by the class implementing Stoppable.
   * If the resources and subsystems are already stopped then
   * invoking this has no effect.
   *
   * @param reason for stopping.
   */
  public void stop(final String reason);

  /**
   * Returns status about whether the thread was stopped.
   *
   * @return true if stopped; false otherwise.
   */
  public boolean isStopped();

}
