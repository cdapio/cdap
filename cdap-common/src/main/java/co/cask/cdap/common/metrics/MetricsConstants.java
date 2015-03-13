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

package co.cask.cdap.common.metrics;

/**
 * Constants for metrics.
 */
public final class MetricsConstants {

  private MetricsConstants() {
  }

  public static final String FLOWLET_INPUT = "system.process.tuples.read";
  public static final String FLOWLET_PROCESSED = "system.process.events.processed";
  public static final String FLOWLET_EXCEPTIONS = "system.process.errors";

  public static final String PROCEDURE_INPUT = "system.query.requests";
  public static final String PROCEDURE_PROCESSED = "system.query.processed";
  public static final String PROCEDURE_EXCEPTIONS = "system.query.failures";

  public static final String SERVICE_INPUT = "system.requests.count";
  public static final String SERVICE_PROCESSED = "system.response.successful.count";
  public static final String SERVICE_EXCEPTIONS = "system.response.server.error.count";

}
