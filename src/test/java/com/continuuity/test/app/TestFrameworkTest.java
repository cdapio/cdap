package com.continuuity.test.app;

import com.continuuity.internal.test.RuntimeStats;
import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestFrameworkTest extends AppFabricTestBase {

  @Test
  public void test() throws InterruptedException, IOException {
    ApplicationManager applicationManager = deployApplication(new WordCountApp2());

    FlowManager flowManager = applicationManager.startFlow("WordCountFlow");

    StreamWriter streamWriter = applicationManager.getStreamWriter("text");

    for (int i = 0; i < 1; i++) {
      streamWriter.send("testing message");
    }

    RuntimeStats.dump(System.out);

    TimeUnit.SECONDS.sleep(2);

    RuntimeStats.dump(System.out);
  }

}
