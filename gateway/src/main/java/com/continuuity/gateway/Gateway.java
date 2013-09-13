package com.continuuity.gateway;

import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.Server;
import com.continuuity.common.service.ServerException;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.auth.PassportVPCAuthenticator;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metadata.MetadataService;
import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Gateway is the front door to the Continuuity platform. It provides a
 * data interface for external clients to send events to the data fabric.
 * It supports two main patterns:
 * <dl>
 * <dt><strong>Send events to a named queue</strong></dt>
 * <dd>This is supported via various protocols. Every protocol is implemented
 * as a Collector that it registered with the Gateway. The collector can
 * implement any protocol, as long as it can convert the data from that
 * protocol into events. All events are routed to a Consumer that is also
 * registered with the gateway. The Consumer is responsible for persisting
 * the event before returning a response.</dd>
 * <dt><strong>To read data from the data fabric</strong></dt>
 * <dd>This is currently not implemented.</dd>
 * </dl>
 */
public class Gateway implements Server {

  /**
   * This is our Logger instance.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  /**
   * This is the consumer that all collectors will use.
   * Gateway can not function without a valid Consumer.
   */
  @Inject
  private Consumer consumer;

  /**
   * This is the executor that all accessors will use for the data fabric.
   * Gateway can not function without a valid operation executor.
   */
  @Inject
  private OperationExecutor executor;

  /**
   * This is the location factory that all accessors will use for the data fabric.
   * Gateway can not function without a valid location factory.
   */
  @Inject
  private LocationFactory locationFactory;

  // to support early integration with TxDs2
  @Inject
  private DataSetAccessor dataSetAccessor;

  // to support early integration with TxDs2
  @Inject
  private TransactionSystemClient txSystemClient;

  /**
   * This is the executor that all accessors will use for the data fabric.
   * Gateway can not function without a valid operation executor.
   */
  @Inject
  private MetadataService mds;

  @Inject
  private MetaDataStore metaDataStore;

  @Inject
  private StoreFactory storeFactory;

  private Store store;

  @Inject
  private DiscoveryServiceClient discoveryServiceClient;

  @Inject
  private LogReader logReader;

  private GatewayMetrics gatewayMetrics = new GatewayMetrics();

  /**
   * The list of connectors for this Gateway. This list is populated in
   * the configure method.
   */
  private List<Connector> connectorList = new ArrayList<Connector>();

  /**
   * Our Configuration object.
   */
  private CConfiguration myConfiguration;

  /**
   * The cluster name, used for authentication purposes.
   */
  private String clusterName;

  /**
   * The passport client used to perform authentication, if necessary.
   */
  private PassportClient passportClient;

  /**
   * The authenticator used for this Gateway instance.
   */
  private GatewayAuthenticator authenticator;

  /**
   * Get the Gateway's current configuration.
   *
   * @return Our current Configuration
   */
  public CConfiguration getConfiguration() {
    return myConfiguration;
  }

  /**
   * Add a Connector to the Gateway. This connector must be in pristine state
   * and not started yet. The Gateway will start the connector when it starts
   * itself.
   *
   * @param connector The connector to register
   * @throws Exception iff a connector with the same name is already registered
   */
  public void addConnector(Connector connector) throws Exception {

    String name = connector.getName();
    if (name == null) {
      Exception e =
        new IllegalArgumentException("Connector name cannot be null.");
      LOG.error(e.getMessage());
      throw e;
    }

    LOG.info("Adding connector '" + name + "' of type " +
               connector.getClass().getName() + ".");

    if (this.hasNamedConnector(name)) {
      Exception e = new Exception("Connector with name '" + name
                                    + "' already registered. ");
      LOG.error(e.getMessage());
      throw e;
    } else {
      connectorList.add(connector);
    }
  }

  /**
   * Start the gateway. This will also start the Consumer and all the Connectors
   *
   * @throws ServerException If there is no Consumer, or whatever Exception a
   *                         connector throws during start().
   */
  public void start(String[] args, CConfiguration conf) throws
    ServerException {

    // Configure ourselves first
    configure(conf);

    // Check we are in the correct state
    if (this.consumer == null) {
      ServerException es =
        new ServerException("Cannot start Gateway without a Consumer.");
      LOG.error(es.getMessage());
      throw es;

    }
    if (this.executor == null) {
      ServerException es =
        new ServerException(
          "Cannot start Gateway without an Operation Executor.");
      LOG.error(es.getMessage());
      throw es;
    }

    LOG.info("Gateway Starting up.");

    // Start our event consumer
    this.consumer.startConsumer();

    // Now start all our Connectors
    for (Connector connector : this.connectorList) {

      // First, perform connector-type specific initialization
      // For a collector, set the Consumer for its events
      // For an accessor, set the operations executor for access to data fabric
      // TODO: This should probably be done in the addConnector method?
      if (connector instanceof Collector) {
        ((Collector) connector).setConsumer(this.consumer);
      }
      if (connector instanceof MetaDataServiceAware) {
        connector.setMetadataService(this.mds);
      }
      if (connector instanceof MetaDataStoreAware) {
        ((MetaDataStoreAware) connector).setMetadataStore(this.metaDataStore);
      }
      // for many unit-tests it is null. We will figure out better strategy around injection for unit-tests and fix it
      if (store != null && connector instanceof StoreAware) {
        ((StoreAware) connector).setStore(this.store);
      }
      if (connector instanceof DataAccessor) {
        DataAccessor dataAccessor = (DataAccessor) connector;
        dataAccessor.setExecutor(this.executor);
        dataAccessor.setLocationFactory(this.locationFactory);
        dataAccessor.setDataSetAccessor(this.dataSetAccessor);
        dataAccessor.setTxSystemClient(this.txSystemClient);
      }
      if (connector instanceof LogReaderAware) {
        ((LogReaderAware) connector).setLogReader(logReader);
      }
      // all connectors get the meta data service
      connector.setMetadataService(this.mds);

      connector.setGatewayMetrics(this.gatewayMetrics);

      try {
        connector.start();
      } catch (Exception e) {
        throw new ServerException(e.getMessage());
      }

      LOG.info(" Started " + connector.getName() + " connector");
    }
  }

  /**
   * Stop the gateway. This will first stop all our Connectors, and then stop
   * the Consumer.
   */
  public void stop(boolean now) throws ServerException {

    LOG.info("Gateway Shutting down");

    // Stop all our connectors
    for (Connector connector : this.connectorList) {
      try {
        connector.stop();
      } catch (Exception e) {
        throw new ServerException(e.getMessage());
      }
      LOG.info(" " + connector.getName() + " stopped");
    }

    // Stop the consumer
    this.consumer.stopConsumer();
    LOG.info(" Consumer stopped");

    LOG.info("Gateway successfully shut down");

  }

  /**
   * Set the Consumer that all events are routed to.
   *
   * @param consumer The Consumer that all events will be sent to
   * @throws IllegalArgumentException If the consumer object is null
   */
  public void setConsumer(Consumer consumer) {
    if (consumer == null) {
      throw new IllegalArgumentException("'consumer' argument was null");
    }
    LOG.info("Setting Consumer to " + consumer.getClass().getName() + ".");
    this.consumer = consumer;
  }

  /**
   * Set the PassportClient that the GatewayAuthenticator will use.
   *
   * @param passportClient The passport client used for authentication
   * @throws IllegalArgumentException If the passportClient object is null
   */
  public void setPassportClient(PassportClient passportClient) {
    Preconditions.checkNotNull(passportClient);
    LOG.info("Setting PassportClient to " + passportClient.getClass().getName()
               + ".");
    this.passportClient = passportClient;
  }

  /**
   * Set the operations executor that is used for all data fabric access.
   *
   * @param executor The executor to use
   * @throws IllegalArgumentException If the consumer object is null
   */
  public void setExecutor(OperationExecutor executor) {
    Preconditions.checkNotNull(executor);
    LOG.info("Setting Operations Executor to " +
               executor.getClass().getName() + ".");
    this.executor = executor;
    this.mds = new MetadataService(executor);
  }

  public void setDiscoveryServiceClient(
    DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;
  }

  // to be used by unit-tests only
  void setDataSetAccessor(DataSetAccessor dataSetAccessor) {
    this.dataSetAccessor = dataSetAccessor;
  }

  // to be used by unit-tests only
  void setTxSystemClient(TransactionSystemClient txSystemClient) {
    this.txSystemClient = txSystemClient;
  }

  /**
   * Set the gateway's Configuration, then create and configure the connectors.
   *
   * @param configuration The Configuration object that contains the options
   *                      for the Gateway and all its connectors. This can not
   *                      be null.
   * @throws IllegalArgumentException If configuration argument is null.
   */
  private void configure(CConfiguration configuration) throws ServerException {
    Preconditions.checkNotNull(configuration);

    LOG.info("Configuring Gateway..");

    if (this.mds == null) {
      this.mds = new MetadataService(executor);
    }

    if (storeFactory != null) {
      store = storeFactory.create();
    }

    // Save the configuration so we can use it again later
    myConfiguration = configuration;

    // Determine cluster instance name for authentication purposes
    this.clusterName = myConfiguration.get(Constants.CONFIG_CLUSTER_NAME,
                                           Constants.CONFIG_CLUSTER_NAME_DEFAULT);

    // Create the authenticator that will be used by all connectors
    boolean requireAuthentication = this.myConfiguration.getBoolean(
      Constants.CONFIG_AUTHENTICATION_REQUIRED,
      Constants.CONFIG_AUTHENTICATION_REQUIRED_DEFAULT);
    if (requireAuthentication) {
      // Tests may set a passport client, so only create one if it dne
      if (this.passportClient == null) {
        this.passportClient = PassportClient.create(
          myConfiguration.get(PassportConstants.CFG_PASSPORT_SERVER_URI)
        );
      }
      this.authenticator = new PassportVPCAuthenticator(this.clusterName, this.passportClient);
    } else {
      this.authenticator = new NoAuthenticator();
    }

    // Retrieve the list of connectors that we will create
    Collection<String> connectorNames = myConfiguration.
      getStringCollection(Constants.CONFIG_CONNECTORS);

    // For each Connector
    for (String connectorName : connectorNames) {

      // Retrieve the connector's Class
      String connectorClassName = myConfiguration.get(
        Constants.buildConnectorPropertyName(connectorName,
                                             Constants.CONFIG_CLASSNAME));
      // Has the user specified the Class? If not, skip this Connector
      if (connectorClassName == null) {
        LOG.error("No Class property defined for " + connectorName +
                    ". Can not create " + connectorName + ".");
      } else {
        // Instantiate a new Connector and then configure it
        Connector newConnector;
        try {
          // Attempt to load the Class
          newConnector =
            (Connector) Class.forName(connectorClassName).newInstance();
          // Tell it what it's called
          newConnector.setName(connectorName);
        } catch (Exception e) {
          LOG.error("Cannot instantiate class " + connectorClassName + "(" +
                      e.getMessage() + "). Skipping Connector '" +
                      connectorName + "'.");
          continue;
        }

        // Now try to configure the Connector
        try {
          newConnector.configure(myConfiguration);
        } catch (Exception e) {
          LOG.error("Error configuring connector '" + connectorName + "' (" +
                      e.getMessage() + "). Skipping connector '" +
                      connectorName + "'.");
          continue;
        }

        newConnector.setDiscoveryServiceClient(discoveryServiceClient);
        // set the connector's authenticator
        newConnector.setAuthenticator(this.authenticator);

        // Add it to our Connector list
        try {
          this.addConnector(newConnector);
        } catch (Exception e) {
          LOG.error("Error adding connector '" + connectorName + "' (" +
                      e.getMessage() + "). Skipping connector '" +
                      connectorName + "'.");
          // continue // unnecessary
        }
      }
    }
  }

  /**
   * Check whether a connector with the given name is already registered.
   *
   * @param name The name to be checked
   * @return true If a connector with the same name exists
   */
  private boolean hasNamedConnector(String name) {
    for (Connector connector : this.connectorList) {
      if (connector.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }


} // end of Gateway class
