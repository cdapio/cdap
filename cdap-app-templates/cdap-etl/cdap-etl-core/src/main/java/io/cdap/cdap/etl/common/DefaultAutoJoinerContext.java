/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default implementation of AutoJoinerContext.
 */
public class DefaultAutoJoinerContext implements AutoJoinerContext {
  private final Map<String, JoinStage> inputStages;
  private final FailureCollector failureCollector;

  public DefaultAutoJoinerContext(Map<String, JoinStage> inputStages, FailureCollector failureCollector) {
    this.inputStages = Collections.unmodifiableMap(new HashMap<>(inputStages));
    this.failureCollector = failureCollector;
  }

  @Override
  public Map<String, JoinStage> getInputStages() {
    return inputStages;
  }

  @Override
  public FailureCollector getFailureCollector() {
    return failureCollector;
  }

  public static DefaultAutoJoinerContext from(Map<String, Schema> inputSchemas, FailureCollector failureCollector) {
    return new DefaultAutoJoinerContext(inputSchemas.entrySet().stream()
                                          .map(e -> JoinStage.builder(e.getKey(), e.getValue()).build())
                                          .collect(Collectors.toMap(JoinStage::getStageName, s -> s)),
                                        failureCollector);
  }
}
