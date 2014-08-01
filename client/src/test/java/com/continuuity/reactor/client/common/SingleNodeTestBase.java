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

package com.continuuity.reactor.client.common;

import com.continuuity.SingleNodeMain;
import com.continuuity.test.internal.AppFabricTestHelper;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class SingleNodeTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeTestBase.class);

  private SingleNodeMain singleNodeMain;

  @Before
  public void setUp() throws Throwable {
    try {
      singleNodeMain = SingleNodeMain.createSingleNodeMain(true);
      singleNodeMain.startUp();
    } catch (Throwable e) {
      System.err.println("Failed to start singlenode. " + e.getMessage());
      LOG.error("Failed to start singlenode", e);
      if (singleNodeMain != null) {
        singleNodeMain.shutDown();
      }
      throw e;
    }
  }

  @After
  public void tearDown() {
    if (singleNodeMain != null) {
      singleNodeMain.shutDown();
    }
  }

  protected File createAppJarFile(Class<?> cls) {
    return new File(AppFabricTestHelper.createAppJar(cls).toURI());
  }

}
