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

package co.cask.cdap.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ImmutablePairTest {

  static final class Fixture {
    static ImmutablePair<Integer, Integer> a = new ImmutablePair<>(1, 2);
    static ImmutablePair<Integer, String>  b = new ImmutablePair<>(1, "woot");
    static ImmutablePair<String, String>   c = new ImmutablePair<>("me", "you");
    static ImmutablePair<Integer, Integer> d = new ImmutablePair<>(1, 2);
    static ImmutablePair<Integer, Integer> e = new ImmutablePair<>(1, 2);
  }

  @Test
  public void testGetFirst() throws Exception {
    Assert.assertEquals(Integer.class, Fixture.a.getFirst().getClass());
    Assert.assertEquals(1, Fixture.a.getFirst().intValue());
  }

  @Test
  public void testGetSecond() throws Exception {
    Assert.assertEquals(Integer.class, Fixture.a.getSecond().getClass());
    Assert.assertEquals(2, Fixture.a.getSecond().intValue());
  }

  @Test
  public void testToString() throws Exception {
    Assert.assertNotNull(Fixture.b.toString());
    Assert.assertEquals("ImmutablePair{first=1, second=woot}", Fixture.b.toString());
  }

  @Test
  public void testSelf() throws Exception {
    Assert.assertTrue(Fixture.a.equals(Fixture.a));
    Assert.assertTrue(Fixture.b.equals(Fixture.b));
    Assert.assertTrue(Fixture.c.equals(Fixture.c));
  }

  @Test
  public void testIncompatibleTypes()  throws Exception {
    Assert.assertFalse(Fixture.a.equals(Fixture.c));
    Assert.assertFalse(Fixture.a.equals(Fixture.b));
  }

  @Test
  public void testNullReferences() throws Exception {
    Assert.assertFalse(Fixture.a.equals(null));
  }

  @Test
  public void testEqualsIsReflexive() {
    Assert.assertTrue(Fixture.a.equals(Fixture.d));
  }

  @Test
  public void testEqualsIsTransitive() {
    Assert.assertTrue(Fixture.a.equals(Fixture.d));
    Assert.assertTrue(Fixture.d.equals(Fixture.e));
    Assert.assertTrue(Fixture.e.equals(Fixture.a));
  }

  @Test
  public void testEqualsTypeSafe() {
    ImmutablePair<Integer, Boolean> pair1 = new ImmutablePair<>(1, true);
    ImmutablePair<String, byte[]> pair2 = new ImmutablePair<>("1", "true".getBytes());
    Assert.assertFalse(pair1.equals(pair2));
    Assert.assertFalse(pair2.equals(pair1));
    Assert.assertFalse(pair1.equals(new Integer(10)));
  }


  @Test
  public void testHashCodeConsistency() throws Exception {
    int hashcode = Fixture.a.hashCode();
    Assert.assertEquals(hashcode, Fixture.a.hashCode());
    Assert.assertEquals(hashcode, Fixture.a.hashCode());
  }

  @Test
  public void testTwoSameObjectHashCode() throws Exception {
    Assert.assertEquals(Fixture.a.hashCode(), Fixture.d.hashCode());
    Assert.assertEquals(Fixture.d.hashCode(), Fixture.e.hashCode());
    Assert.assertEquals(Fixture.e.hashCode(), Fixture.a.hashCode());
  }

  @Test
  public void testTwoDifferentObjectHashCode() throws Exception {
    Assert.assertTrue(!(Fixture.a.hashCode() == Fixture.c.hashCode()));
  }

}
