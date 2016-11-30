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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.operations.OperationalStatsUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Tests for {@link OperationalStatsHttpHandler}.
 */
public class OperationalStatsHttpHanderTest {

  private final  OperationalStatsHttpHandler handler = new OperationalStatsHttpHandler();

  @BeforeClass
  public static void setup() throws Exception {
   registerBean("one", "string", new OneString());
   registerBean("one", "int", new OneInt());
   registerBean("one", "float", new OneFloat());
   registerBean("two", "string", new TwoString());
   registerBean("two", "int", new TwoInt());
   registerBean("two", "float", new TwoFloat());
  }

  @Test
  public void testReadByName() throws Exception {
    Map<String, Map<String, Object>> expected = new HashMap<>();
    Map<String, Object> inner = new HashMap<>();
    inner.put("OneString", "one");
    expected.put("string", inner);
    inner = new HashMap<>();
    inner.put("OneInt", 1);
    expected.put("int", inner);
    inner = new HashMap<>();
    inner.put("OneFloat", 1.0F);
    expected.put("float", inner);
    Assert.assertEquals(expected, handler.getStats("name", "one", "type"));
    expected = new HashMap<>();
    inner = new HashMap<>();
    inner.put("TwoString", "two");
    expected.put("string", inner);
    inner = new HashMap<>();
    inner.put("TwoInt", 2);
    expected.put("int", inner);
    inner = new HashMap<>();
    inner.put("TwoFloat", 2.0F);
    expected.put("float", inner);
    Assert.assertEquals(expected, handler.getStats("name", "two", "type"));
  }

  @Test
  public void testReadByType() throws Exception {
    Map<String, Map<String, Object>> expected = new HashMap<>();
    Map<String, Object> inner = new HashMap<>();
    inner.put("OneString", "one");
    expected.put("one", inner);
    inner = new HashMap<>();
    inner.put("TwoString", "two");
    expected.put("two", inner);
    Assert.assertEquals(expected, handler.getStats("type", "string", "name"));
    expected = new HashMap<>();
    inner = new HashMap<>();
    inner.put("OneInt", 1);
    expected.put("one", inner);
    inner = new HashMap<>();
    inner.put("TwoInt", 2);
    expected.put("two", inner);
    Assert.assertEquals(expected, handler.getStats("type", "int", "name"));
    expected = new HashMap<>();
    inner = new HashMap<>();
    inner.put("OneFloat", 1.0F);
    expected.put("one", inner);
    inner = new HashMap<>();
    inner.put("TwoFloat", 2.0F);
    expected.put("two", inner);
    Assert.assertEquals(expected, handler.getStats("type", "float", "name"));
  }

  @Test
  public void testInvalid() throws Exception {
    try {
      handler.getStats(null, "foo", "don'tcare");
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      handler.getStats("bar", null, "don'tcare");
      Assert.fail();
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  private static void registerBean(String name, String type, Object bean) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Hashtable<String, String> properties = new Hashtable<>();
    properties.put("name", name);
    properties.put("type", type);
    mbs.registerMBean(bean, new ObjectName(OperationalStatsUtils.JMX_DOMAIN, properties));
  }

  public interface OneStringMXBean {
    @SuppressWarnings("unused")
    String getOneString();
  }

  public static class OneString implements OneStringMXBean {
    @Override
    public String getOneString() {
      return "one";
    }
  }

  public interface OneIntMXBean {
    @SuppressWarnings("unused")
    int getOneInt();
  }

  public static class OneInt implements OneIntMXBean {
    @Override
    public int getOneInt() {
      return 1;
    }
  }

  public interface OneFloatMXBean {
    @SuppressWarnings("unused")
    float getOneFloat();
  }

  public static class OneFloat implements OneFloatMXBean {
    @Override
    public float getOneFloat() {
      return 1.0F;
    }
  }

  public interface TwoStringMXBean {
    @SuppressWarnings("unused")
    String getTwoString();
  }

  public static class TwoString implements TwoStringMXBean {
    @Override
    public String getTwoString() {
      return "two";
    }
  }

  public interface TwoIntMXBean {
    @SuppressWarnings("unused")
    int getTwoInt();
  }

  public static class TwoInt implements TwoIntMXBean {
    @Override
    public int getTwoInt() {
      return 2;
    }
  }

  public interface TwoFloatMXBean {
    @SuppressWarnings("unused")
    float getTwoFloat();
  }

  public static class TwoFloat implements TwoFloatMXBean {
    @Override
    public float getTwoFloat() {
      return 2.0F;
    }
  }
}
