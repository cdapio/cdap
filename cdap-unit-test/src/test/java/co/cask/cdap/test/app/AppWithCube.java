/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler;
import co.cask.cdap.api.dataset.lib.cube.Cube;

/**
 *
 */
public class AppWithCube extends AbstractApplication {
  static final String CUBE_NAME = "cube";
  static final String SERVICE_NAME = "service";

  @Override
  public void configure() {
    DatasetProperties props = DatasetProperties.builder()
      .add("dataset.cube.resolutions", "1,60")
      .add("dataset.cube.aggregation.agg1.dimensions", "user,action")
      .add("dataset.cube.aggregation.agg1.requiredDimensions", "user,action").build();
    createDataset(CUBE_NAME, Cube.class, props);

    addService(SERVICE_NAME, new CubeHandler());
  }

  /**
   *
   */
  public static final class CubeHandler extends AbstractCubeHttpHandler {
    @UseDataSet(CUBE_NAME)
    private Cube cube;

    @Override
    protected Cube getCube() {
      return cube;
    }
  }
}
