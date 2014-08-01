/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics.runtime.MetricsTwillRunnable;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link com.continuuity.metrics.runtime.MetricsTwillRunnable}.
 */
public class MetricsTwillRunnableTest {

  @Test
  public void testInjection() throws Exception {
    Injector injector = MetricsTwillRunnable.createGuiceInjector(CConfiguration.create(), HBaseConfiguration.create());
    Assert.assertNotNull(injector);
  }
}
