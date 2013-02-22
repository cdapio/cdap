package com.continuuity.gateway;

public class AppFabricCollectorTest {

  //TODO: MARIO IS WORKING ON FIXING THIS TEST.
//
//  private final static String CONTINUUITY_API_KEY = PassportConstants.CONTINUUITY_API_KEY_HEADER;
//  private final static String CONTINUUITY_JAR_FILE_NAME = "X-Continuuity-JarFileName";
//
//  static final OperationContext context = OperationContext.DEFAULT;
//
//  /**
//   * this is the executor for all access to the data fabric
//   */
//  private OperationExecutor executor;
//
//  /**
//   * the rest connector we will use in the tests
//   */
//  private AppFabricRestConnector connector;
//
//  /**
//   * the rest collector we will use in the clear test
//   */
//  private RestCollector collector;
//
//  private static AppFabricServer server;
//  private static LocationFactory lf;
//  private static StoreFactory sFactory;
//  private static CConfiguration configuration;
//
//  /**
//   * Set up in-memory data fabric
//   */
//  @Before
//  public void setup() throws InterruptedException {
//
//    configuration = CConfiguration.create();
//    configuration.setInt(com.continuuity.common.conf.Constants.CFG_APP_FABRIC_SERVER_PORT, 45000);
//    configuration.set("app.output.dir", "/tmp/app");
//    configuration.set("app.tmp.dir", "/tmp/temp");
//
//    Injector injector = Guice.createInjector(
//      new BigMamaModule(configuration),
//      new DataFabricModules().getInMemoryModules()
//    );
//    server = injector.getInstance(AppFabricServer.class);
//
//    server.start();
//
//    ListenableFuture<Service.State> future = server.start();
//
//    // Create location factory.
//    lf = injector.getInstance(LocationFactory.class);
//
//    // Create store
//    sFactory = injector.getInstance(StoreFactory.class);
//
//
//
//  } // end of setupGateway
//
//  /**
//   * Create a new rest connector with a given name and parameters
//   *
//   * @param name   The name for the connector
//   * @param prefix The path prefix for the URI
//   * @param middle The path middle for the URI
//   * @return the connector's base URL for REST requests
//   */
//  String setupConnector(String name, String prefix, String middle)
//      throws Exception {
//    // bring up a new connector
//    AppFabricRestConnector restConnector = new AppFabricRestConnector();
//    restConnector.setName(name);
//    restConnector.setAuthenticator(new NoAuthenticator());
//    // find a free port
//    int port = PortDetector.findFreePort();
//    // configure it
//    CConfiguration configuration = new CConfiguration();
//    configuration.setInt(Constants.buildConnectorPropertyName(name,
//        Constants.CONFIG_PORT), port);
//    configuration.set(Constants.buildConnectorPropertyName(name,
//        Constants.CONFIG_PATH_PREFIX), prefix);
//    configuration.set(Constants.buildConnectorPropertyName(name,
//        Constants.CONFIG_PATH_MIDDLE), middle);
//    restConnector.configure(configuration);
//    // start the connector
//    restConnector.start();
//    // all fine
//    this.connector = restConnector;
//    return "http://localhost:" + port + prefix + middle;
//  }
//
//  static final ByteBuffer emptyBody = ByteBuffer.wrap(new byte[0]);
//
//  /**
//   * This tests that the connector can be stopped and restarted
//   */
//  @Test @Ignore
//  public void testStopRestart() throws Exception {
//    // configure an connector
//    String baseUrl = setupConnector("access.rest", "/continuuity", "/table/");
//    int port = this.connector.getHttpConfig().getPort();
//    // test that ping works
//    Assert.assertEquals(200, TestUtil.sendGetRequest("http://localhost:" + port + "/ping"));
//    // stop the connector
//    this.connector.stop();
//    // verify that GET fails now. Should throw an exception
//    try {
//      TestUtil.sendGetRequest("http://localhost:" + port + "/ping");
//      Assert.fail("Expected HttpHostConnectException because connector was " +
//          "stopped.");
//    } catch (HttpHostConnectException e) {
//      // this is expected
//    }
//    // restart the connector
//    this.connector.start();
//    // submit a GET with existing key -> 200 OK
//    Assert.assertEquals(200, TestUtil.sendGetRequest("http://localhost:" + port + "/ping"));
//    // and finally shut down
//    this.connector.stop();
//  }
//
//  @Test @Ignore
//  public void testDeploy() throws Exception {
//    // setup connector
//    String baseUrl = setupConnector("connector.rest", "/continuuity", "/rest-app/");
//    String deployUrl = this.connector.getHttpConfig().getBaseUrl() + "deploy";
//    // setup collector
//    int port = this.connector.getHttpConfig().getPort();
//
//    // Create a local jar - simulate creation of application archive.
//    Location deployedJar = lf.create(
//      JarFinder.getJar(ToyApp.class, TestHelper.getManifestWithMainClass(ToyApp.class))
//    );
//    deployedJar.deleteOnExit();
//
//    Map<String,String> headers= Maps.newHashMap();
//    headers.put(CONTINUUITY_API_KEY,"api-key-example");
//    headers.put(CONTINUUITY_JAR_FILE_NAME, deployedJar.getName());
//
////    BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100*1024);
//
//    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//    try {
//      ByteStreams.copy(deployedJar.getInputStream(), bos);
//    } finally {
//      bos.close();
//    }
//
////    byte [] bytes;
////     try {
////    byte[] chunk;
////    while (true) {
////      chunk = is.read();
////      if(chunk.length==0) break;
////      bos.write(chunk);
////    }
////    bos.flush();
////    bytes=bos.toByteArray();
////    } finally {
////    is.close();
////    bos.close();
////    }
//
//    Assert.assertEquals(200, TestUtil.sendPostRequest(deployUrl, bos.toByteArray(), headers));
//  }
//  @After
//  public void stop() {
//    Service.State state = server.stopAndWait();
//    Assert.assertTrue(state == Service.State.TERMINATED);
//  }
}

