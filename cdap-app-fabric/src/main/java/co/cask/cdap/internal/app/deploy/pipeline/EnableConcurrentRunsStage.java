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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 * Enables concurrent runs for an application.
 */
public class EnableConcurrentRunsStage extends AbstractStage<ApplicationWithPrograms> {
  private final PreferencesStore preferencesStore;

  public EnableConcurrentRunsStage(PreferencesStore preferencesStore) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.preferencesStore = preferencesStore;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true");
    preferencesStore.setProperties(input.getId().getNamespaceId(), input.getSpecification().getName(), properties);
    emit(input);
  }
}
