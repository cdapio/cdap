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

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;

/**
 * Simple Identity Transform for testing.
 */
public class IdentityTransform<A, B> extends Transform<A, B, A, B> {

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("IdentityTransform");
  }

  @Override
  public void transform(A keyIn, B valueIn, Emitter<A, B> emitter) {
    emitter.emit(keyIn, valueIn);
  }
}
