/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.store;

import co.cask.cdap.common.kerberos.OwnerStore;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * Tests for {@link DefaultOwnerStore}.
 */
public class InMemoryOwnerStoreTest extends OwnerStoreTest {

  private static OwnerStore ownerStore;

  @BeforeClass
  public static void createInjector() {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(OwnerStore.class).to(InMemoryOwnerStore.class);
      }
    });
    ownerStore = injector.getInstance(OwnerStore.class);
  }

  @Override
  public OwnerStore getOwnerStore() {
    return ownerStore;
  }
}
