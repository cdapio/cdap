package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class LogCollectorTest {

  String makeMessage(int i) {
    return "This is message " + i + ".";
  }

  static String makeTempDir() {
    Random rand = new Random(System.currentTimeMillis());
    while (true) {
      int num = rand.nextInt(Integer.MAX_VALUE);
      String dirName = "tmp_coll" + num;
      File dir = new File(dirName);
      if (dir.mkdir())
        return dirName;
    }
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test @Ignore
  public void testCollection() throws IOException {

    // create a new temp dir for the log root
    File prefix = tempFolder.newFolder();

    CConfiguration config = CConfiguration.create();
    config.set(Constants.CFG_LOG_COLLECTION_ROOT, prefix.getAbsolutePath());

    LogCollector collector = new LogCollector(config);

    String logtag = "a:b:c";

    collector.log(new LogEvent(logtag, "ERROR", "this is an error message"));
    collector.close();

    List<String> lines = collector.tail(logtag, 1024);
    for (String line : lines) {
      System.out.println(line);
    }
    Assert.assertEquals(1, lines.size());
  }

}
