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

import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Test for {@link Cask360Entity}.
 */
public class TestCask360Entity {
  private static final Logger LOG = LoggerFactory.getLogger(TestCask360Entity.class);

  @Test
  public void testCombining() throws Exception {

    Cask360Entity one = makeTestEntityTwo("1");
    LOG.info("One      [0] = " + one.toString());
    Cask360Entity two = makeTestEntityTwoOverlap("1");
    LOG.info("Two      [0] = " + two.toString());
    Cask360Entity truth = makeTestEntityTwoCombined("1");
    LOG.info("Truth    [0] = " + truth.toString());

    Cask360Entity combined = new Cask360Entity("1");
    LOG.info("Combined [0] = " + combined.toString());
    combined.write(one);
    LOG.info("Combined [1] = " + combined.toString());
    combined.write(two);
    LOG.info("Combined [2] = " + combined.toString());

    String msg = "\nCombined:\n" + combined.toString() + "\nTruth:\n" + truth.toString();
    LOG.info(msg);
    Assert.assertTrue("Combined does not match expected result:\n" + msg, combined.equals(truth));

    one = makeTestEntityTimeTwoA("1");
    two = makeTestEntityTimeTwoB("1");
    truth = makeTestEntityTimeTwoAB("1");

    combined = new Cask360Entity("1");
    combined.write(one);
    combined.write(two);

    msg = "Combined:\n" + combined.toString() + "\nTruth:\n" + truth.toString();
    LOG.info(msg);
    Assert.assertTrue("Combined does not match expected result:\n" + msg, combined.equals(truth));

  }

  @Test
  public void testFromString() {

    // Output sample

    Cask360Entity simple = new Cask360Entity("myID");
    simple.write("group1", "key1", "value1");
    simple.write("group1", "key2", "value2");
    simple.write("group2", "key3", "value3");
    LOG.info("Simple: " + simple.toString());

    // Verify two lines (CSV and JSON) turn into the same entity

    String csvLine = "3,mygroup,mykey,myval";
    String jsonLine = "{'id':'3','data':{'mygroup':{'type':'map','data':{'mykey':'myval'}}}}";
    Cask360Entity csvEntity = Cask360Entity.fromString(csvLine);
    Cask360Entity jsonEntity = Cask360Entity.fromString(jsonLine);
    Assert.assertTrue("CSV and JSON representations did not match as entities\n" + csvEntity.toString() + "\n"
        + jsonEntity.toString(), csvEntity.equals(jsonEntity));

    // Verify slightly more complex JSON turns into expected entity

    jsonLine = "{'id':'4','data':{'group1':{'type':'map','data':{'key1':'val1', " +
        "'key2':'val2'}},'group2':{'type':'map','data':{'key3':'val3'}}}}";
    Cask360Entity manualEntity = new Cask360Entity("4");
    manualEntity.write("group1", "key1", "val1");
    manualEntity.write("group1", "key2", "val2");
    manualEntity.write("group2", "key3", "val3");
    System.out.println(manualEntity.toString());
    jsonEntity = Cask360Entity.fromString(jsonLine);
    Assert.assertTrue("Complex JSON representation did not match expected entity", manualEntity.equals(jsonEntity));

    // Verify complex entities can properly be converted to/from a String (MAP)

    Cask360Entity origA = makeTestEntityOne("5");
    Cask360Entity origB = makeTestEntityTwo("6");
    Cask360Entity origC = makeTestEntityTwoOverlap("7");
    Cask360Entity origD = makeTestEntityTwoCombined("8");
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origA.equals(Cask360Entity.fromString(origA.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origB.equals(Cask360Entity.fromString(origB.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origC.equals(Cask360Entity.fromString(origC.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origD.equals(Cask360Entity.fromString(origD.toString())));

    // Verify complex entities can properly be converted to/from a String (TIME)

    origA = makeTestEntityTimeOne("T5");
    origB = makeTestEntityTimeTwoA("T6");
    origC = makeTestEntityTimeTwoB("T7");
    origD = makeTestEntityTimeTwoAB("T8");
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origA.equals(Cask360Entity.fromString(origA.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origB.equals(Cask360Entity.fromString(origB.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origC.equals(Cask360Entity.fromString(origC.toString())));
    Assert.assertTrue("Complex entity was not correctly stringified and reconstructed",
        origD.equals(Cask360Entity.fromString(origD.toString())));

  }

  public static Cask360Entity makeTestEntityOne(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapOne = new TreeMap<String, String>();
    mapOne.put("a", "b");
    mapOne.put("c", "d");
    mapOne.put("e", "f");
    testData.put("one", new Cask360Group("one", new Cask360GroupDataMap(mapOne)));
    Map<String, String> mapTwo = new TreeMap<String, String>();
    mapTwo.put("g", "h");
    testData.put("two", new Cask360Group("two", new Cask360GroupDataMap(mapTwo)));
    Map<String, String> mapThree = new TreeMap<String, String>();
    mapThree.put("i", "j");
    mapThree.put("k", "l");
    testData.put("three", new Cask360Group("three", new Cask360GroupDataMap(mapThree)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwo(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "f");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "l");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwoOverlap(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwoCombined(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTimeOne(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "aV");
    mapOne1.put("bK", "bV");
    mapOne1.put("cK", "cV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "aV");
    mapOne2.put("cK", "cV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "zV");
    mapOne3.put("cK", "cV");
    mapOne.put(789L, mapOne3);
    testData.put("one", new Cask360Group("one", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(2L, mapFour1);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTimeTwoA(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "aV");
    mapOne1.put("bK", "bV");
    mapOne1.put("cK", "cV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "aV");
    mapOne2.put("cK", "cV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "zV");
    mapOne3.put("cK", "cV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(2L, mapFour1);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    return new Cask360Entity(id, testData);
  }

  private static final long now_ms = System.currentTimeMillis();
  private static final long now_ns = System.nanoTime();

  public static Cask360Entity makeTestEntityTimeTwoB(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("cK", "CV");
    mapOne1.put("aK", "AV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "AV");
    mapOne2.put("dK", "DV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "ZV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(1L, mapFour1);
    Map<String, String> mapFour2 = new TreeMap<String, String>();
    mapFour2.put("xK", "xV");
    mapFour2.put("yK", "yV");
    mapFour.put(3L, mapFour2);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    Map<Long, Map<String, String>> mapFive = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFive1 = new TreeMap<String, String>();
    mapFive1.put("xK", "xV");
    mapFive1.put("yK", "yV");
    mapFive.put(now_ms, mapFive1);
    Map<String, String> mapFive2 = new TreeMap<String, String>();
    mapFive2.put("xK", "xV");
    mapFive2.put("yK", "yV");
    mapFive.put(now_ns, mapFive2);
    testData.put("Tfive", new Cask360Group("Tfive", new Cask360GroupDataTime(mapFive)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTimeTwoAB(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();

    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "AV");
    mapOne1.put("cK", "CV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "AV");
    mapOne2.put("dK", "DV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "ZV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(1L, mapFour1);
    Map<String, String> mapFour2 = new TreeMap<String, String>();
    mapFour2.put("xK", "xV");
    mapFour2.put("yK", "yV");
    mapFour.put(3L, mapFour2);
    Map<String, String> mapFour3 = new TreeMap<String, String>();
    mapFour3.put("xK", "xV");
    mapFour3.put("yK", "yV");
    mapFour.put(2L, mapFour3);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    Map<Long, Map<String, String>> mapFive = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFive1 = new TreeMap<String, String>();
    mapFive1.put("xK", "xV");
    mapFive1.put("yK", "yV");
    mapFive.put(now_ms, mapFive1);
    Map<String, String> mapFive2 = new TreeMap<String, String>();
    mapFive2.put("xK", "xV");
    mapFive2.put("yK", "yV");
    mapFive.put(now_ns, mapFive2);
    testData.put("Tfive", new Cask360Group("Tfive", new Cask360GroupDataTime(mapFive)));
    return new Cask360Entity(id, testData);
  }
}
