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

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.ProgramLifecycle;

/**
 * Defines the Flowlet interface. A Flowlet should provide the
 * functionality for initializing and configuring itself.
 */
public interface Flowlet extends ProgramLifecycle<FlowletContext> {
  /**
   * Provides an interface to configure a Flowlet.
   * <p>
   *   This method could possibly be called multiple times and hence
   *   should not be used for allocating resources needed at run-time. This
   *   method is generally called in configuration phase of the flowlet
   *   which happens during application deployment.
   * </p>
   * <p>
   *   The {@link FlowletSpecification} returned from this method is available
   *   at runtime through the {@link FlowletContext} which is given through
   *   the {@link #initialize(FlowletContext)} method.
   * </p>
   *
   * @return An instance of {@link FlowletSpecification}
   */
  FlowletSpecification configure();

  /**
   *  Initializes a Flowlet.
   *  <p>
   *    This method will be called only once per {@link Flowlet} instance..
   *  </p>
   *  @param context An instance of {@link FlowletContext}
   *  @throws Exception If there is any error during initialization.
   */
  @Override
  void initialize(FlowletContext context) throws Exception;

  /**
   * Destroy is the last thing that gets called before the flowlet is
   * shutdown. So, if there are any cleanups then they can be specified here.
   *
   * <p>
   *   This method will be called only when there are no more events beings processed
   *   by the flowlet.
   * </p>
   */
  @Override
  void destroy();
}
