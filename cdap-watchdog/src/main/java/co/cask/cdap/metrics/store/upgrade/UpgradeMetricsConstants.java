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
package co.cask.cdap.metrics.store.upgrade;

/**
 *
 */
public final class UpgradeMetricsConstants {
  public static final String DEFAULT_ENTITY_TABLE_NAME_V1 = "metrics.entity";

  // for migration purpose
  public static final String EMPTY_TAG = "-";
  public static final int DEFAULT_CONTEXT_DEPTH = 6;
  public static final int DEFAULT_METRIC_DEPTH = 4;
  public static final int DEFAULT_TAG_DEPTH = 3;
}
