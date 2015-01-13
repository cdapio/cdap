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

package co.cask.cdap.common.guice;

import co.cask.cdap.internal.io.ASMDatumWriterFactory;
import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.DatumWriterFactory;
import co.cask.cdap.internal.io.FieldAccessorFactory;
import co.cask.cdap.internal.io.ReflectionDatumReaderFactory;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.api.data.schema.SchemaGenerator;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * A Guice module for IO related classes bindings.
 */
public class IOModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);
    expose(SchemaGenerator.class);

    bind(FieldAccessorFactory.class).to(ASMFieldAccessorFactory.class).in(Scopes.SINGLETON);
    bind(DatumWriterFactory.class).to(ASMDatumWriterFactory.class).in(Scopes.SINGLETON);

    expose(DatumWriterFactory.class);

    // Note: Need to add the DatumReader counter parts when those are refactored to use ASM as well.
    bind(DatumReaderFactory.class).to(ReflectionDatumReaderFactory.class).in(Scopes.SINGLETON);
    expose(DatumReaderFactory.class);
  }
}
