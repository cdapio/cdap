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

package io.cdap.cdap.common.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.internal.io.ASMDatumWriterFactory;
import io.cdap.cdap.internal.io.ASMFieldAccessorFactory;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.FieldAccessorFactory;
import io.cdap.cdap.internal.io.ReflectionDatumReaderFactory;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.internal.io.SchemaGenerator;

/**
 * A Guice module for IO related classes bindings.
 */
public class IOModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);
    expose(SchemaGenerator.class);

    bind(FieldAccessorFactory.class).to(ASMFieldAccessorFactory.class);
    bind(DatumWriterFactory.class).to(ASMDatumWriterFactory.class);

    expose(DatumWriterFactory.class);

    // Note: Need to add the DatumReader counter parts when those are refactored to use ASM as well.
    bind(DatumReaderFactory.class).to(ReflectionDatumReaderFactory.class).in(Scopes.SINGLETON);
    expose(DatumReaderFactory.class);
  }
}
