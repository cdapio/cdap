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

package com.continuuity.app.deploy;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;

/**
 * This interface is used for defining the execution of configure either in sandbox jvm for distributed mode
 * or within a thread in a single node.
 * <p/>
 * <p>
 * This interface extends from {@link Callable} with the intent that the callee is responsible for making
 * sure that he runs this in a thread that allows to timeout the execution of configure.
 * </p>
 */
public interface Configurator {

  /**
   * Invokes the configurator.
   * <p>
   * It's responsible for taking a JAR file and running the configure in secured sandbox or non secured sandbox.
   * </p>
   *
   * @return A instance of future that callee can timeout.
   */
  ListenableFuture<ConfigResponse> config();
}
