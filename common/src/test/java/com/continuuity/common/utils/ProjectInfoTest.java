/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for testing {@link ProjectInfo}.
 */
public class ProjectInfoTest {

  @Test
  public void testInfo() {
    Assert.assertTrue(ProjectInfo.getVersion().getMajor() > 0);
    Assert.assertTrue(ProjectInfo.getVersion().getBuildTime() > 0L);
  }

  @Test
  public void testVersion() {
    ProjectInfo.Version version = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    Assert.assertEquals(2, version.getMajor());
    Assert.assertEquals(1, version.getMinor());
    Assert.assertEquals(0, version.getFix());
    Assert.assertTrue(version.isSnapshot());
    Assert.assertEquals(12345L, version.getBuildTime());

    Assert.assertEquals("2.1.0-SNAPSHOT-12345", version.toString());
  }

  @Test
  public void testVersionCompare() {
    // Major version
    ProjectInfo.Version version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    ProjectInfo.Version version2 = new ProjectInfo.Version("3.0.0-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Minor version
    version1 = new ProjectInfo.Version("2.0.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Fix version
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.1-SNAPSHOT-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Snapshot, non-snapshot
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-12345");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Buildtime
    version1 = new ProjectInfo.Version("2.1.0-12345");
    version2 = new ProjectInfo.Version("2.1.0-12346");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);

    // Buildtime with snapshot
    version1 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12345");
    version2 = new ProjectInfo.Version("2.1.0-SNAPSHOT-12346");

    Assert.assertTrue(version1.compareTo(version1) == 0);
    Assert.assertTrue(version1.compareTo(version2) < 0);
    Assert.assertTrue(version2.compareTo(version1) > 0);
  }
}
