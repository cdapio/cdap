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

package io.cdap.cdap.internal.io;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ASMDatumWriterFactory}.
 */
public class AsmDatumWriterFactoryTest {

  /**
   * Simple record for encoding tests.
   */
  public static final class SampleRecord {
    private final int id;
    private final String label;

    public SampleRecord(int id, String label) {
      this.id = id;
      this.label = label;
    }
  }

  @Test
  public void testCreateAndEncodeAndExceptionPropagation() throws Exception {
    Schema validSchema = new ReflectionSchemaGenerator().generate(SampleRecord.class);
    ASMDatumWriterFactory factory = new ASMDatumWriterFactory(new ASMFieldAccessorFactory());
    DatumWriter<SampleRecord> writer = factory.create(TypeToken.of(SampleRecord.class), validSchema);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.encode(new SampleRecord(7, "seven"), new BinaryEncoder(out));

    ReflectionDatumReader<SampleRecord> reader = new ReflectionDatumReader<>(
        validSchema, TypeToken.of(SampleRecord.class));
    SampleRecord decoded = reader.read(
        new BinaryDecoder(new ByteArrayInputStream(out.toByteArray())), validSchema);
    Assert.assertEquals(7, decoded.id);
    Assert.assertEquals("seven", decoded.label);

    // 1. Incompatible schema causes DatumWriterGenerator failure -> UncheckedExecutionException
    // rethrown unwrapped by Throwables.propagate / throwIfUnchecked.
    Schema missingFieldSchema = Schema.recordOf("SampleRecord",
        Schema.Field.of("nonExistentField", Schema.of(Schema.Type.INT)));
    try {
      factory.create(TypeToken.of(SampleRecord.class), missingFieldSchema);
      Assert.fail("Expected UncheckedExecutionException unwrapped");
    } catch (UncheckedExecutionException expected) {
      Assert.assertNotNull(expected.getCause());
    }

    // 2. Failing FieldAccessorFactory throws inside generated DatumWriter constructor ->
    // Constructor.newInstance throws checked InvocationTargetException -> wrapped in RuntimeException.
    IllegalStateException accessorFailure = new IllegalStateException("accessor factory failure");
    ASMDatumWriterFactory failingFactory = new ASMDatumWriterFactory((type, fieldName) -> {
      throw accessorFailure;
    });
    try {
      failingFactory.create(TypeToken.of(SampleRecord.class), validSchema);
      Assert.fail("Expected RuntimeException wrapping InvocationTargetException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof InvocationTargetException);
      Assert.assertSame(accessorFailure, caught.getCause().getCause());
    }
  }
}
