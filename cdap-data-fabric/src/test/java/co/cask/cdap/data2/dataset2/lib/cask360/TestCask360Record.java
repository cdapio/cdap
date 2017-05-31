/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Record;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link Cask360Record}.
 */
public class TestCask360Record {

  @Test
  public void testToAndFromString() {
    
    // Verify two lines (CSV and JSON) turn into the same entity

    String csvLine = "3,mygroup,mykey,myval";
    String jsonLine = "{'id':'3','group':'mygroup','key':'mykey','value':'myval'}";
    Cask360Record csvEntity = Cask360Record.fromString(csvLine);
    Cask360Record jsonEntity = Cask360Record.fromString(jsonLine);
    System.out.println("csv: " + csvEntity.toString() + "\njson:" + jsonEntity.toString());
    Assert.assertTrue("CSV and JSON representations did not match as entities",
        csvEntity.equals(jsonEntity));
    
    // Verify complex entities can properly be converted to/from a String

    Cask360Record origA = new Cask360Record("1", "2", "3", "4");
    Cask360Record origB = new Cask360Record("5", "6", "7", "8");

    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origA.equals(Cask360Record.fromString(origA.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origB.equals(Cask360Record.fromString(origB.toString())));
    
  }

}
