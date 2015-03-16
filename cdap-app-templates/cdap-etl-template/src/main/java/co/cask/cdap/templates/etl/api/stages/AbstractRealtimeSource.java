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

package co.cask.cdap.templates.etl.api.stages;

import co.cask.cdap.templates.etl.api.SourceConfigurer;
import co.cask.cdap.templates.etl.api.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;

/**
 *
 */
public abstract class AbstractRealtimeSource<O> implements RealtimeSource<O> {

  private SourceContext context;

  @Override
  public void configure(SourceConfigurer configurer) {
    configurer.setName(this.getClass().getSimpleName());
    configurer.setDescription("");
  }

  @Override
  public void initialize(SourceContext context) {
    this.context = context;
  }

  @Override
  public void onSuspend() {
    //no-op
  }

  @Override
  public void onResume(int oldInstance, int newInstance) {
    //no-op
  }

  @Override
  public void destroy() {
    //no-op
  }

  protected SourceContext getContext() {
    return context;
  }
}
