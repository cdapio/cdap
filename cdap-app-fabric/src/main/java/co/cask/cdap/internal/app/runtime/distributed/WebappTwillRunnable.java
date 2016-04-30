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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.internal.app.runtime.webapp.ExplodeJarHttpHandler;
import co.cask.cdap.internal.app.runtime.webapp.JarHttpHandler;
import co.cask.cdap.internal.app.runtime.webapp.WebappHttpHandlerFactory;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.apache.twill.api.TwillContext;

/**
 * Twill runnable wrapper for webapp.
 */
final class WebappTwillRunnable extends AbstractProgramTwillRunnable<WebappProgramRunner> {

  WebappTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Module createModule(TwillContext context) {
    return Modules.combine(super.createModule(context), new AbstractModule() {
      @Override
      protected void configure() {
        // Create webapp http handler factory.
        install(new FactoryModuleBuilder()
                  .implement(JarHttpHandler.class, ExplodeJarHttpHandler.class)
                  .build(WebappHttpHandlerFactory.class));
      }
    });
  }
}
