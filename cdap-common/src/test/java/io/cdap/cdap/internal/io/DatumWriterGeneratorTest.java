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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.internal.asm.ByteCodeClassLoader;
import io.cdap.cdap.internal.asm.ClassDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DatumWriterGenerator}.
 */
public class DatumWriterGeneratorTest {

  /**
   * Interface record type to exercise interface getter code generation in encodeRecord.
   */
  public interface PersonView {
    String getName();

    boolean isActive();
  }

  private static final class SimpleRecord {
    private final int count;

    SimpleRecord(int count) {
      this.count = count;
    }
  }

  private static final class SelfReferencingNode {
    private SelfReferencingNode next;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGenerateForInterfaceAndCircularReference() throws Exception {
    Schema viewSchema = Schema.recordOf("PersonView",
        Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("active", Schema.of(Schema.Type.BOOLEAN)));

    ClassDefinition def = new DatumWriterGenerator()
        .generate(TypeToken.of(PersonView.class), viewSchema);
    ByteCodeClassLoader loader = new ByteCodeClassLoader(getClass().getClassLoader());
    Class<DatumWriter<PersonView>> writerClass =
        (Class<DatumWriter<PersonView>>) loader.addClass(def).loadClass(def.getClassName());
    DatumWriter<PersonView> writer = writerClass
        .getConstructor(Schema.class, FieldAccessorFactory.class)
        .newInstance(viewSchema, new ASMFieldAccessorFactory());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.encode(new PersonView() {
      @Override
      public String getName() {
        return "alice";
      }

      @Override
      public boolean isActive() {
        return true;
      }
    }, new BinaryEncoder(out));
    Assert.assertTrue(out.size() > 0);

    // Circular reference throws IOException.
    Schema nodeSchema = new ReflectionSchemaGenerator().generate(SelfReferencingNode.class);
    DatumWriter<SelfReferencingNode> nodeWriter =
        new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
            .create(TypeToken.of(SelfReferencingNode.class), nodeSchema);
    SelfReferencingNode cycle = new SelfReferencingNode();
    cycle.next = cycle;
    try {
      nodeWriter.encode(cycle, new BinaryEncoder(new ByteArrayOutputStream()));
      Assert.fail("Expected IOException on circular reference");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("Circular reference"));
    }
  }

  @Test
  public void testEncodeRecordCheckedAndUncheckedExceptionPropagation() {
    // 1. Concrete class missing schema field -> checked NoSuchFieldException wrapped in RuntimeException.
    Schema badClassSchema = Schema.recordOf("SimpleRecord",
        Schema.Field.of("missingField", Schema.of(Schema.Type.INT)));
    try {
      new DatumWriterGenerator().generate(TypeToken.of(SimpleRecord.class), badClassSchema);
      Assert.fail("Expected RuntimeException wrapping NoSuchFieldException");
    } catch (RuntimeException caught) {
      Assert.assertTrue(caught.getCause() instanceof NoSuchFieldException);
    }

    // 2. Interface missing getter -> unchecked IllegalArgumentException rethrown unwrapped.
    Schema badInterfaceSchema = Schema.recordOf("PersonView",
        Schema.Field.of("unknownProp", Schema.of(Schema.Type.STRING)));
    try {
      new DatumWriterGenerator().generate(TypeToken.of(PersonView.class), badInterfaceSchema);
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getCause() instanceof NoSuchMethodException);
    }
  }
}
