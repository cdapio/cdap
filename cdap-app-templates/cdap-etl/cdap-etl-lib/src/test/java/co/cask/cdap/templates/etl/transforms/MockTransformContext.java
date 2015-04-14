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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.TransformContext;

import java.util.Map;

/**
 * Mock context for unit tests
 */
public class MockTransformContext implements TransformContext {
  private final Map<String, String> args;

  public MockTransformContext(Map<String, String> args) {
    this.args = args;
  }

  @Override
  public StageSpecification getSpecification() {
    return null;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return args;
  }
}
