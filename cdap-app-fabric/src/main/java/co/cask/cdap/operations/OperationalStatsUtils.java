/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Utilities for use in collection and retrieval of {@link OperationalStats}.
 */
public final class OperationalStatsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsUtils.class);

  public static final String JMX_DOMAIN = "co.cask.cdap.operations";
  public static final String SERVICE_NAME_KEY = "name";
  public static final String STAT_TYPE_KEY = "type";
  public static final String STAT_TYPE_INFO = "info";

  @Nullable
  static OperationalExtensionId getOperationalExtensionId(OperationalStats operationalStats) {
    String serviceName = operationalStats.getServiceName();
    String statType = operationalStats.getStatType();
    if (Strings.isNullOrEmpty(serviceName) && Strings.isNullOrEmpty(statType)) {
      return null;
    }
    if (!Strings.isNullOrEmpty(serviceName)) {
      serviceName = serviceName.toLowerCase();
    } else {
      LOG.warn("Found operational stat without service name - {}. This stat will not be discovered by service name.",
               operationalStats.getClass().getName());
    }
    if (!Strings.isNullOrEmpty(statType)) {
      statType = statType.toLowerCase();
    } else {
      LOG.warn("Found operational stat without stat type - {}. This stat will not be discovered by stat type.",
               operationalStats.getClass().getName());
    }
    return new OperationalExtensionId(serviceName, statType);
  }

  private OperationalStatsUtils() {
    // prevent instantiation.
  }
}
