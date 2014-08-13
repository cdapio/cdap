/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.explore.service;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

/**
 *
 */
public class ExploreServiceUtilsTest {

  @Test
  public void testHiveVersion() throws Exception {
    // This would throw an exception if it didn't pass
    ExploreServiceUtils.checkHiveSupportWithSecurity(new Configuration(), this.getClass().getClassLoader());
  }

  @Test
  public void hijackHiveConfFileTest() throws Exception {
    ExploreServiceUtils.hijackHiveConfFile(new File("tmp"));
  }

}
