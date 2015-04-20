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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;

import java.util.Map;

/**
 * Mock RealtimeContext for tests.
 */
public class MockRealtimeContext implements RealtimeContext {

  @Override
  public StageSpecification getSpecification() {
    return null;
  }

  @Override
  public Metrics getMetrics() {
    return NoopMetrics.INSTANCE;
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getInstanceCount() {
    return 0;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return null;
  }
}
