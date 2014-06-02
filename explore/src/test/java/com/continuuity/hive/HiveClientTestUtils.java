package com.continuuity.hive;

import com.continuuity.hive.client.HiveClient;

import junit.framework.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 *
 */
public class HiveClientTestUtils {

  public static void assertCmdFindPattern(HiveClient hiveClient, String cmd, String pattern) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    hiveClient.sendCommand(cmd, out, null);
    String res = out.toString("UTF-8");
    try {
      Assert.assertTrue(Pattern.compile(pattern, Pattern.DOTALL).matcher(res).find());
    } finally {
      out.close();
    }
  }
}
