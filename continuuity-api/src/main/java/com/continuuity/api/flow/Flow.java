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

package com.continuuity.api.flow;

import com.continuuity.api.Processor;

/**
 * Flow is a collection of {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} that are
 * wired together into a Direct Acylic Graph (DAG).
 *
 * <p>
 *   Implement this interface to create a flow. The {@link #configure()} method will be
 *   invoked during deployment time and it returns a {@link FlowSpecification} to specify how to
 *   configure the flow.
 * </p>
 *
 * See the <i>Continuuity Reactor Developer Guide</i> and the Reactor example applications.
 * @see FlowSpecification
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 */
public interface Flow extends Processor {
  /**
   * Configure the {@link Flow} by returning a {@link FlowSpecification}.
   *
   * @return An instance of {@link FlowSpecification}.
   */
  FlowSpecification configure();
}
