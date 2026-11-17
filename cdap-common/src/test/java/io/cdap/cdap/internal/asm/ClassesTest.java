/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.asm;

import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

/**
 * Tests {@link Classes} bytecode helpers on Java 11 classes with nestmate attributes.
 */
public class ClassesTest {

  /**
   * Sample Java 11 nest host with nested and anonymous classes for ASM7 rewrite tests.
   */
  public static class SampleNestHost {

    private final String secret = "nest-secret";

    public void shouldBeNoop() {
      throw new IllegalStateException("Method should have been rewritten to no-op");
    }

    public String callNestedPrivateAccess() throws Exception {
      return new NestedMember().readSecret();
    }

    /**
     * Nested member accessing private field of {@link SampleNestHost} via JEP 181 nestmates.
     */
    public class NestedMember implements Callable<String> {

      @Override
      public String call() {
        return secret;
      }

      private String readSecret() {
        return call();
      }
    }
  }

  @Test
  public void testRewriteMethodToNoopOnJava11Nestmates() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    for (Class<?> cls : new Class<?>[]{
        ClassesTest.class, SampleNestHost.class, SampleNestHost.NestedMember.class}) {
      String resource = cls.getName().replace('.', '/') + ".class";
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
        Assert.assertNotNull(is);
        byte[] rewritten = Classes.rewriteMethodToNoop(
            cls.getName(), is, Collections.singleton("shouldBeNoop"));
        classLoader.addClass(new ClassDefinition(rewritten, Type.getInternalName(cls)));
      }
    }

    Class<?> hostClass = classLoader.loadClass(SampleNestHost.class.getName());
    Object host = hostClass.getDeclaredConstructor().newInstance();
    hostClass.getMethod("shouldBeNoop").invoke(host);
    Assert.assertEquals("nest-secret",
        hostClass.getMethod("callNestedPrivateAccess").invoke(host));
  }
}
