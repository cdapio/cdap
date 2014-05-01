package com.continuuity.data.hive;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

public class HiveUtilTest {

  @Test
  public void testHiveSchemaFor() throws Exception {

    Assert.assertEquals("(first:INT,second:VARCHAR)",
                        HiveUtil.hiveSchemaFor(new TypeToken<ImmutablePair<Integer, String>>() {}.getType()));
  }
}
