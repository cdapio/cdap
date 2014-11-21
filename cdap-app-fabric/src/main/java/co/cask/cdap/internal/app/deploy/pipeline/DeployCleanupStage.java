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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;

import java.io.Closeable;

/**
 * Stage for cleaning up resources created during deployment time.
 */
public class DeployCleanupStage extends AbstractStage<Closeable> {

  public DeployCleanupStage() {
    super(TypeToken.of(Closeable.class));
  }

  @Override
  public void process(Closeable input) throws Exception {
    input.close();
  }
}
