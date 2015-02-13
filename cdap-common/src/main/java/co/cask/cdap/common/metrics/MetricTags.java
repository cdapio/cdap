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

package co.cask.cdap.common.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for mapping MetricTags to their Code Names and vice versa.
 */
public enum MetricTags {

  RUN_ID("run"),
  INSTANCE_ID("ins"),
  COMPONENT("cmp"),
  STREAM("str"),
  DATASET("ds"),
  SERVICE("srv"),
  SERVICE_RUNNABLE("srn"),
  HANDLER("hnd"),
  METHOD("mtd"),
  MR_TASK_TYPE("mrt"),
  APP("app"),
  PROGRAM("prg"),
  PROGRAM_TYPE("ptp"),
  FLOWLET("flt"),
  FLOWLET_QUEUE("flq"),
  CLUSTER_METRICS("cls"),
  NAMESPACE("ns"),
  // who emitted: user vs system (scope is historical name)
  SCOPE("scp");

  private String codeName;

  private static final Map<String, MetricTags> CODE_TO_FULL_NAME_MAP;

  static {
    CODE_TO_FULL_NAME_MAP = new HashMap<String, MetricTags>();
    for (MetricTags type : MetricTags.values()) {
      CODE_TO_FULL_NAME_MAP.put(type.getCodeName(), type);
    }
  }

  private MetricTags(String codeName) {
    this.codeName = codeName;
  }

  public static String valueOfCodeName(String codeName) {
    return CODE_TO_FULL_NAME_MAP.get(codeName).getCodeName();
  }

  public String getCodeName() {
    return codeName;
  }

  public static String getCodeFromFullName(String fullName) {
    MetricTags tag = valueOf(fullName.toUpperCase());
    return tag.getCodeName();
  }
}
