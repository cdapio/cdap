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
package io.cdap.cdap.data.runtime.main;

import static org.junit.Assert.assertFalse;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.Test;

/**
 * Test the guice module overrides for the {@link TransactionServiceTwillRunnable}.
 */
public class TransactionServiceGuiceTest {

  @Test
  public void testGuiceInjector() {
    Injector injector = TransactionServiceTwillRunnable.createGuiceInjector(CConfiguration.create(),
                                                                            new Configuration(), "");
    // get one tx manager
    TransactionManager txManager1 = injector.getInstance(TransactionManager.class);
    // get a second tx manager
    TransactionManager txManager2 = injector.getInstance(TransactionManager.class);
    // these should be two separate instances
    assertFalse(txManager1 == txManager2);
  }
}
