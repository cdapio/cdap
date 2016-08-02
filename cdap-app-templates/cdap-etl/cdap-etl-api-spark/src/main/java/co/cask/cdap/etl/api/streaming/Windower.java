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

package co.cask.cdap.etl.api.streaming;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;

import java.io.Serializable;

/**
 * Windowing plugin.
 */
@Beta
public abstract class Windower implements PipelineConfigurable, Serializable {

  public static final String PLUGIN_TYPE = "windower";

  private static final long serialVersionUID = -7949508317034247623L;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    // no-op
  }

  /**
   * @return the width of the window in seconds. Must be a multiple of the underlying batch interval.
   */
  public abstract long getWidth();

  /**
   * @return the slide interval of the window in seconds. Must be a multiple of the underlying batch interval.
   */
  public abstract long getSlideInterval();
}
