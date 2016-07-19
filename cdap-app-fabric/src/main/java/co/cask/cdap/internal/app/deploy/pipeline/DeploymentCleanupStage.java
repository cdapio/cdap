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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.pipeline.Context;
import co.cask.cdap.pipeline.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The final stage during the deployment to cleanup all intermediate resources created.
 */
public class DeploymentCleanupStage implements Stage {

  private static final Logger LOG = LoggerFactory.getLogger(DeploymentCleanupStage.class);

  @Override
  public void process(Context ctx) throws Exception {
    for (String key : ctx.getPropertyKeys()) {
      closeIfCloseable(ctx.getProperty(key));
    }

    closeIfCloseable(ctx.getUpStream());
    closeIfCloseable(ctx.getDownStream());
  }

  private void closeIfCloseable(Object obj) {
    if (obj instanceof AutoCloseable) {
      try {
        ((AutoCloseable) obj).close();
      } catch (Exception e) {
        LOG.warn("Exception raised during deployment cleanup. Object to be closed: {}", obj, e);
      }
    }
  }
}
