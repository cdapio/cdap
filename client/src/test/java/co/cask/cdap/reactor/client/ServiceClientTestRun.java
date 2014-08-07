package co.cask.cdap.reactor.client;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.reactor.client.app.FakeApp;
import co.cask.cdap.reactor.client.app.FakeRunnable;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.apache.twill.discovery.Discoverable;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link ServiceClient}
 */
@Category(XSlowTests.class)
public class ServiceClientTestRun extends ClientTestBase {
  private ApplicationClient appClient;
  private ServiceClient serviceClient;
  private ProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    ClientConfig config = new ClientConfig("localhost");
    appClient = new ApplicationClient(config);
    serviceClient = new ServiceClient(config);
    programClient = new ProgramClient(config);
  }

  @Test
  public void testDiscover() throws Exception {
    appClient.deploy(createAppJarFile(FakeApp.class));
    programClient.start(FakeApp.NAME, ProgramType.SERVICE, FakeRunnable.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.SERVICE, FakeRunnable.NAME);
    List<Discoverable> discoverables = serviceClient.discover(FakeApp.NAME, FakeRunnable.NAME, FakeRunnable.NAME);
    assertEquals(discoverables.size(), 1);
    assertNotNull(discoverables.get(0).getSocketAddress());
  }
}
