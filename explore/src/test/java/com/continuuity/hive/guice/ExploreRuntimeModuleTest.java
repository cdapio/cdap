package com.continuuity.hive.guice;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 *
 */
public class ExploreRuntimeModuleTest {

  @Test
  public void testGetHiveClassPath() throws Exception {
    Assert.assertEquals(Lists.newArrayList(new File("/path1").toURI().toURL(), new File("/path2").toURI().toURL()),
                        Lists.newArrayList(ExploreRuntimeModule.getClassPath("/path1:/path2")));

    Assert.assertNull(ExploreRuntimeModule.getClassPath(null));

    Assert.assertEquals(Lists.newArrayList(new File("/path100").toURI().toURL()),
                        Lists.newArrayList(ExploreRuntimeModule.getClassPath("/path100")));
  }

  @Test
  public void printEnv() {
    System.out.println(System.getenv());
  }
}
