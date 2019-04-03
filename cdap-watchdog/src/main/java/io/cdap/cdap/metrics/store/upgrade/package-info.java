/*
 * Copyright © 2015 Cask Data, Inc.
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

/**
 * Used for migrating metrics data from CDAP versions earlier than 2.8 to CDAP-2.8.
 * The Metrics system and the the data-format has changed in CDAP-2.8, In order to port the existing metrics,
 * this package contains classes that reads existing metrics data, parses them and
 * finally adds the metrics data to {@link co.cask.cdap.api.metrics.MetricStore}.
 */
package co.cask.cdap.metrics.store.upgrade;
