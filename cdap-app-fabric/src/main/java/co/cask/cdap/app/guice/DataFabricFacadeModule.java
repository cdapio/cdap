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
package co.cask.cdap.app.guice;

import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.LineageWriterDataFabricFacade;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

/**
 * A private module for creating bindings for DataFabricFacadeFactory
 */
public final class DataFabricFacadeModule extends PrivateModule {

  @Override
  protected void configure() {

    // Creates a DataFabricFacadeFactory injection for creating DataFabricFacade of different types.
    install(
      new FactoryModuleBuilder()
        .implement(DataFabricFacade.class, LineageWriterDataFabricFacade.class)
        .build(DataFabricFacadeFactory.class)
    );
    expose(DataFabricFacadeFactory.class);
  }
}
