/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.inject.Inject;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsOperationFactory;

/**
 * Implementation of {@link OperationRunner} to run an operation in the same service.
 */
public class InMemoryOperationRunner extends AbstractOperationRunner {

  private final OperationStatePublisher statePublisher;

  /**
   * Default constructor.
   *
   * @param statePublisher Publishes the current operation state.
   */
  @Inject
  public InMemoryOperationRunner(OperationStatePublisher statePublisher,
      PullAppsOperationFactory pullOperationFactory) {
    super(pullOperationFactory);
    this.statePublisher = statePublisher;
  }

  @Override
  public OperationController run(OperationRunDetail detail) throws IllegalStateException {
    LongRunningOperationContext context = new LongRunningOperationContext(detail.getRunId(), statePublisher);
    OperationDriver driver = new OperationDriver(createOperation(detail), context);
    OperationController controller = new InMemoryOperationController(context.getRunId(),
        statePublisher, driver);
    driver.start();
    return controller;
  }
}
