/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.metrics.guice;

import com.google.common.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DistributedMetricsClientModule}.
 */
public class DistributedMetricsClientModuleTest {

  @Test
  public void testProvidesDatumWriterSuccess() {
    Injector ioInjector = Guice.createInjector(
        new ConfigModule(CConfiguration.create()), new IOModule());
    DistributedMetricsClientModule module = new DistributedMetricsClientModule();

    DatumWriter<MetricValues> writer = module.providesDatumWriter(
        ioInjector.getInstance(SchemaGenerator.class),
        ioInjector.getInstance(DatumWriterFactory.class));
    Assert.assertNotNull(writer);
  }

  @Test
  public void testCheckedUnsupportedTypeExceptionWrappedInRuntimeException() throws Exception {
    final UnsupportedTypeException checkedFailure =
        new UnsupportedTypeException("unsupported metric schema");
    SchemaGenerator schemaGenerator = Mockito.mock(SchemaGenerator.class);
    Mockito.when(schemaGenerator.generate(MetricValues.class)).thenThrow(checkedFailure);

    DistributedMetricsClientModule module = new DistributedMetricsClientModule();
    try {
      module.providesDatumWriter(schemaGenerator, Mockito.mock(DatumWriterFactory.class));
      Assert.fail("Expected RuntimeException wrapping UnsupportedTypeException");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedFailure, actual.getCause());
    }
  }

  @Test
  public void testUncheckedRuntimeExceptionIsNotDoubleWrapped() throws Exception {
    final IllegalArgumentException uncheckedFailure =
        new IllegalArgumentException("writer factory error");
    SchemaGenerator schemaGenerator = Mockito.mock(SchemaGenerator.class);
    Schema dummySchema = Schema.of(Schema.Type.STRING);
    Mockito.when(schemaGenerator.generate(MetricValues.class)).thenReturn(dummySchema);

    DatumWriterFactory writerFactory = Mockito.mock(DatumWriterFactory.class);
    Mockito.when(writerFactory.create(TypeToken.of(MetricValues.class), dummySchema))
        .thenThrow(uncheckedFailure);

    DistributedMetricsClientModule module = new DistributedMetricsClientModule();
    try {
      module.providesDatumWriter(schemaGenerator, writerFactory);
      Assert.fail("Expected IllegalArgumentException without double-wrapping");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }
  }
}
