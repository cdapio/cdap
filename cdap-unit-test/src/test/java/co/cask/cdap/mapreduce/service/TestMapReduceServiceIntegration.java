package co.cask.cdap.mapreduce.service;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.app.MyKeyValueTableDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestMapReduceServiceIntegration extends TestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager applicationManager = deployApplication(TetsMapReduceServiceIntegrationApp.class);
    try {
      ServiceManager serviceManager = applicationManager.startService(TetsMapReduceServiceIntegrationApp.SERVICE_NAME);
      serviceStatusCheck(serviceManager, true);

      DataSetManager<MyKeyValueTableDefinition.KeyValueTable> dictionary = applicationManager.getDataSet(TetsMapReduceServiceIntegrationApp.INPUT_DATASET);
      dictionary.get().write("key1", "Two words");
      dictionary.get().write("key2", "Plus three words");
      dictionary.flush();

      MapReduceManager mrManager = applicationManager.startMapReduce(TetsMapReduceServiceIntegrationApp.MR_NAME);
      mrManager.waitForFinish(180, TimeUnit.SECONDS);

      DataSetManager<MyKeyValueTableDefinition.KeyValueTable> datasetManager = applicationManager.getDataSet(TetsMapReduceServiceIntegrationApp.OUTPUT_DATASET);
      MyKeyValueTableDefinition.KeyValueTable results = datasetManager.get();

      String total = results.get(TetsMapReduceServiceIntegrationApp.TOTAL_WORDS_COUNT);
      Assert.assertEquals(5, Integer.parseInt(total));
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
