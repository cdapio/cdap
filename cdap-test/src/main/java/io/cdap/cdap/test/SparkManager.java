/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.api.spark.Spark;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Instance for this class is for managing a running {@link Spark}.
 */
public interface SparkManager extends ProgramManager<SparkManager> {

  /**
   * Used to discover the endpoint of that exposed by `SparkHttpServiceHandler`.
   * @return URL of the endpoint or {@code null} if the service is not available
   */
  URL getServiceURL();

  /**
   * Used to discover the endpoint of that exposed by `SparkHttpServiceHandler` which allows a custom timeout
   * value to wait for the service to be available.
   *
   * @param timeout how long to wait before giving up, in unit of {@code timeoutUnit}
   * @param timeoutUnit a {@link TimeUnit} to interpret the value of {@code timeout}
   * @return URL of the endpoint or {@code null} if the service is not available
   */
  URL getServiceURL(long timeout, TimeUnit timeoutUnit);

}
