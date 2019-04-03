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
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Collections;

/**
 * The base implementation of {@link AppenderContext} that provides integration with CDAP system.
 */
public abstract class AbstractAppenderContext extends AppenderContext {

  private final LocationFactory locationFactory;
  private final TransactionRunner transactionRunner;
  private final MetricsContext metricsContext;

  protected AbstractAppenderContext(TransactionRunner transactionRunner,
                                    LocationFactory locationFactory,
                                    MetricsCollectionService metricsCollectionService) {
    this.locationFactory = locationFactory;
    this.transactionRunner = transactionRunner;
    this.metricsContext = metricsCollectionService.getContext(Collections.emptyMap());
  }

  @Override
  public final TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Override
  public final LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public final MetricsContext getMetricsContext() {
    return metricsContext;
  }
}
