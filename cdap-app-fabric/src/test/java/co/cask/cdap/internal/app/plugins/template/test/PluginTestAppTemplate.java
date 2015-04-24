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

package co.cask.cdap.internal.app.plugins.template.test;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.worker.AbstractWorker;

/**
 * An {@link ApplicationTemplate} for testing plugin supports.
 */
public class PluginTestAppTemplate extends ApplicationTemplate<PluginTestAppTemplate.TemplateConfig> {

  public static final class TemplateConfig {

  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.addWorker(new PluginTestWorker());
  }

  public static final class PluginTestWorker extends AbstractWorker {

    @Override
    public void run() {
      // No-op
    }
  }
}
