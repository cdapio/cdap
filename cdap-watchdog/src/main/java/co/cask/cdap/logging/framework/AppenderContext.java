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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.dataset.DatasetManager;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Context provided to {@link Appender} via the {@link Appender#setContext(Context)} method.
 */
public abstract class AppenderContext extends LoggerContext implements Transactional {

  public abstract int getInstanceId();

  public abstract int getInstanceCount();

  public abstract DatasetManager getDatasetManager();

  public abstract LocationFactory getLocationFactory();
}
