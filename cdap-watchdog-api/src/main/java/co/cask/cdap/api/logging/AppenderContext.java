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

package co.cask.cdap.api.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.metrics.MetricsContext;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Context provided to {@link Appender} via the {@link Appender#setContext(Context)} method.
 */
public abstract class AppenderContext extends LoggerContext implements Transactional {

  /**
   * Returns the current instance id of the log framework.
   */
  public abstract int getInstanceId();

  /**
   * Returns the total number of log framework instances running.
   */
  public abstract int getInstanceCount();

  /**
   * Returns a {@link DatasetManager} for interacting with the dataset system.
   */
  public abstract DatasetManager getDatasetManager();

  /**
   * Returns a {@link LocationFactory} that the log framework is running on.
   */
  public abstract LocationFactory getLocationFactory();

  /**
   * Returns a {@link MetricsContext} for emitting metrics.
   * @return
   */
  public abstract MetricsContext getMetricsContext();
}
