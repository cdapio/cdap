/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.framework;

import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A {@link AppenderContext} used in local mode.
 */
public class LocalAppenderContext extends AbstractAppenderContext {

  public LocalAppenderContext(TransactionRunner transactionRunner,
                              LocationFactory locationFactory,
                              MetricsCollectionService metricsCollectionService) {
    super(transactionRunner, locationFactory, metricsCollectionService);
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getInstanceCount() {
    return 1;
  }
}
