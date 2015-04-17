package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkerManager;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLRealtimeTemplate}.
 */
public class ETLWorkerTest extends TestBase {
  private static ApplicationManager templateManager;

  @BeforeClass
  public static void setupTests() {
    templateManager = deployApplication(ETLRealtimeTemplate.class);
  }

  @Test
  public void testSimpleConfig() throws Exception {
    ApplicationTemplate<ETLRealtimeConfig> appTemplate = new ETLRealtimeTemplate();

    ETLStage source = new ETLStage(HelloSource.class.getSimpleName(), ImmutableMap.<String, String>of());
    ETLStage sink = new ETLStage(NoOpSink.class.getSimpleName(), ImmutableMap.<String, String>of());
    List<ETLStage> transforms = Lists.newArrayList();
    ETLRealtimeConfig adapterConfig = new ETLRealtimeConfig(1, source, sink, transforms);
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    Map<String, String> workerArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      workerArgs.put(entry.getKey(), entry.getValue());
    }

    WorkerManager workerManager = templateManager.startWorker(ETLWorker.class.getSimpleName(), workerArgs);
    TimeUnit.MINUTES.sleep(1);
    workerManager.stop();
    templateManager.stopAll();
  }
}
