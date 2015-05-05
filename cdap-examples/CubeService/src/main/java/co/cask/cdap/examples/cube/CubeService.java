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

package co.cask.cdap.examples.cube;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler;
import co.cask.cdap.api.dataset.lib.cube.Cube;

/**
 * Simple Cube App with Cube Service
 */
public class CubeService extends AbstractApplication {

  @Override
  public void configure() {
    setName("CubeServiceApp");
    setDescription("Cube App with Cube Service");
    addService("CubeService", new CubeHandler());
  }

  /**
   * Cube Handler
   */
  public final class CubeHandler extends AbstractCubeHttpHandler {
    @UseDataSet("logEventCube")
    private Cube cube;

    @Override
    protected Cube getCube() {
      return cube;
    }
  }

}
