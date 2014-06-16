package com.continuuity.explore.client;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

/**
 * Test DatasetExploreFacade.
 */
public class DatasetExploreFacadeTest {
  enum Enum { FOO, BAR }

  @SuppressWarnings("unused")
  static class Record {
    int a;
    long b;
    boolean c;
    float d;
    double e;
    String f;
    byte[] g;
    Enum[] h;
    Collection<Boolean> i;
    Map<Integer, String> j;
  }

  @SuppressWarnings("unused")
  static class Int {
    Integer value;
  }

  @SuppressWarnings("unused")
  static class Longg {
    long value;
  }

  @Test
  public void testHiveSchemaFor() throws Exception {

    Assert.assertEquals("(value INT)", DatasetExploreFacade.hiveSchemaFor(Int.class));
    Assert.assertEquals("(value BIGINT)", DatasetExploreFacade.hiveSchemaFor(Longg.class));
    Assert.assertEquals("(first INT,second STRING)",
                        DatasetExploreFacade.hiveSchemaFor(new TypeToken<ImmutablePair<Integer, String>>() {
                        }.getType()));
    Assert.assertEquals("(a INT,b BIGINT,c BOOLEAN,d FLOAT,e DOUBLE,f STRING,g BINARY," +
                          "h ARRAY<STRING>,i ARRAY<BOOLEAN>,j MAP<INT,STRING>)",
                        DatasetExploreFacade.hiveSchemaFor(Record.class));

  }

}
