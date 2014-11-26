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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import com.google.inject.name.Named;

/**
 * A Guice assisted inject factory for creating different types of {@link DataFabricFacade}.
 */
public interface DataFabricFacadeFactory {

  /**
   * Creates a {@link DataFabricFacade} for the given program, with transaction supports.
   */
  DataFabricFacade create(Program program, DatasetInstantiator instantiator);

  /**
   * Creates a {@link DataFabricFacade} for the given program, without transaction supports.
   */
  @Named("transaction.off") DataFabricFacade createNoTransaction(Program program, DatasetInstantiator instantiator);
}
