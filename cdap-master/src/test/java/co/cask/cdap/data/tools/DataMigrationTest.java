/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data.tools;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Data migration tests
 */
public class DataMigrationTest {

  @Test
  public void testArgumentsParsing() throws Exception {
    // in testMigrationParse, we return false if there are any issues with arguments and true if the arguments are valid
    List<String[]> validArgumentList = ImmutableList.of(new String[] {"metrics", "--keep-old-metrics-data"},
                                                        new String[] {"metrics"},
                                                        new String[] {"help"});

    List<String[]> invalidArgumentList = ImmutableList.of(new String[] {"metrics", "--keep-all-data"},
                                                          new String[] {"metrics", "-1", "-2", "-3"}
                                                          );
    // valid cases
    for (String[] arguments : validArgumentList) {
      Assert.assertTrue(DataMigration.testMigrationParse(arguments));
    }
    // invalid cases
    for (String[] arguments : invalidArgumentList) {
      Assert.assertFalse(DataMigration.testMigrationParse(arguments));
    }
  }
}
