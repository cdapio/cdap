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

package io.cdap.cdap.common.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies Guice 4.2.3 (no_aop) diagnostics and {@link Binding#toString()} work under Guava 33
 * without {@code Objects.toStringHelper} linkage failures or shaded CGLIB reflective access.
 */
public class GuiceGuavaCompatibilityTest {

  @Test
  public void testBindingToStringAndCreationErrorFormatting() {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(String.class).toInstance("guava33-ok");
      }

      @Provides
      Integer providePackagePrivateValue() {
        return 42;
      }
    });
    Binding<String> binding = injector.getBinding(Key.get(String.class));
    String desc = binding.toString();
    Assert.assertTrue(desc, desc.contains("guava33-ok"));
    Assert.assertEquals(Integer.valueOf(42), injector.getInstance(Integer.class));

    try {
      Class.forName("com.google.inject.internal.cglib.core.$ReflectUtils");
      Assert.fail("Expected Guice no_aop artifact without shaded CGLIB");
    } catch (ClassNotFoundException expected) {
      // Expected for guice-4.2.3-no_aop
    }

    try {
      Guice.createInjector(new AbstractModule() {
        @Override
        protected void configure() {
          bind(String.class).toInstance("a");
          bind(String.class).toInstance("b");
        }
      });
      Assert.fail("Expected CreationException");
    } catch (CreationException err) {
      String msg = err.getMessage();
      Assert.assertTrue("Unexpected CreationException message: " + msg,
                        msg != null && msg.contains("java.lang.String"));
    }
  }
}
