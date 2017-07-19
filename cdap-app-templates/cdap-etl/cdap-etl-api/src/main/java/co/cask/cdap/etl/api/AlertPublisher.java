/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import java.util.Iterator;

/**
 * A pipeline stage that can publish any alerts emitted by the previous stage.
 */
public abstract class AlertPublisher implements PipelineConfigurable, StageLifecycle<AlertPublisherContext> {
  public static final String PLUGIN_TYPE = "alertpublisher";
  private AlertPublisherContext context;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    // no-op
  }

  @Override
  public void initialize(AlertPublisherContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {
    // no-op
  }

  /**
   * Publish a collection of alerts.
   *
   * @param alerts iterator over the alerts
   * @throws Exception if there was an exception publishing the alerts
   */
  public abstract void publish(Iterator<Alert> alerts) throws Exception;

  protected AlertPublisherContext getContext() {
    return context;
  }
}
