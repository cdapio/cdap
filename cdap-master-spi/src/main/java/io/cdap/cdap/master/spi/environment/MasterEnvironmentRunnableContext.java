/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.environment;

import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Context object available to {@link MasterEnvironmentRunnable} for access to CDAP resources.
 */
public interface MasterEnvironmentRunnableContext {

  /**
   * Returns the {@link LocationFactory} used by the CDAP.
   */
  LocationFactory getLocationFactory();

  /**
   * Opens a {@link HttpURLConnection} for the given resource path.
   */
  HttpURLConnection openHttpURLConnection(String resource) throws IOException;


  /**
   * Instantiates a {@link TwillRunnable} from a provided class name.
   *
   * @return the twill runnable instance
   */
  TwillRunnable instantiateTwillRunnable(String className);
}
