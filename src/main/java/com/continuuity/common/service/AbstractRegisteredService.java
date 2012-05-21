package com.continuuity.common.service;

import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServiceDiscoveryClientImpl;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * This abstract class makes it easy to build a registered service.
 * <p>
 *   RegisteredService provides an ability to register the service with the central
 *   service discovery system and also provides a command port that can be accessed
 *   locally to investigate the service.
 * </p>
 */
public abstract class AbstractRegisteredService implements RegisteredService {
  private static final Logger Log = LoggerFactory.getLogger(AbstractRegisteredService.class);

  /**
   * Name of the service
   */
  private String service = "named-but-notnamed";

  /**
   * Command port server associated with the service.
   */
  private CommandPortServer server;

  /**
   * Service thread returned from starting of the service. The service thread is actually managed
   * by the AbstractRegisteredService.
   */
  private Thread serviceThread;

  /**
   * Base class constructor with service name being provided.
   * @param service name
   */
  public AbstractRegisteredService(String service) {
    this.service = service;
  }

  /**
   * Returns name of the service
   * @return name of the service.
   */
  public String getServiceName() {
    return this.service;
  }

  /**
   * Adds a command listener to the command port server.
   * <p>
   *   NOTE: If the same command is registered twice the latest one wins.
   * </p>
   * @param command to be registered.
   * @param description of the command that is being registered.
   * @param listener to be associated with command, that is invoked when command port sees that.
   */
  public void addCommandListener(String command, String description, CommandPortServer.CommandListener listener) {
    server.addListener(command, description, listener);
  }

  /**
   * Starts the service
   *
   * @param args arguments for the service
   * @param conf instance of configuration object.
   * @throws RegisteredServiceException
   */
  public final void start(String[] args, Configuration conf) throws RegisteredServiceException {
    String zkEnsemble = conf.get("zookeeper.quorum");
    Preconditions.checkNotNull(zkEnsemble);

    try {
      server = new CommandPortServer(service);

      addCommandListener("stop", "Stops the service", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          stop(true);
          return "Done";
        }
      });

      addCommandListener("ruok", "Checks if service is up", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          if(ruok()) {
            return "imok";
          }
          return "i am in trouble";
        }
      });

      ImmutablePair<Map<String, String>, Integer> serviceArgs = configure(args, conf);
      if(serviceArgs == null) {
        throw new RegisteredServiceException("configuration of service failed.");
      }

      ServiceDiscoveryClient client = new ServiceDiscoveryClientImpl(zkEnsemble);
      client.register(service, serviceArgs.getSecond().intValue(), serviceArgs.getFirst());

      serviceThread = start();
      if(serviceThread == null) {
        throw new RegisteredServiceException("Thread returned from start is null");
      }
      serviceThread.start();
      server.serve();
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to register the server with discovery service, shutting down. Reason {}", e.getMessage());
      stop(true);
      throw new RegisteredServiceException("Unable to register the server with discovery service");
    } catch (CommandPortServer.CommandPortException e) {
      Log.warn("Error starting the command port service. Service not started. Reason : {}", e.getMessage());
      throw new RegisteredServiceException("Could not start command port service. Reason : " + e.getMessage());
    } catch (IOException e) {
      Log.error("Error starting the command port service. Reason : {}", e.getMessage());
      throw new RegisteredServiceException("Could not start command port service. Reason : " + e.getMessage());
    }
  }

  /**
   * Stops the service.
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public final void stop(boolean now) {
    stop();
  }

  /**
   * Extending class should implement this API. Should do everything to start a service in the Thread.
   * @return Thread instance that will be managed by the service.
   */
  protected abstract Thread start();

  /**
   * Should be implemented by the class extending {@link AbstractRegisteredService} to stop the service.
   */
  protected abstract void stop();

  /**
   * Override to investigate the service and return status.
   * @return true if service is running and good; false otherwise.
   */
  protected abstract boolean ruok();

  /**
   * Configures the service.
   *
   * @param args from command line based for configuring service
   * @param conf Configuration instance passed around.
   * @return Pair of args for registering the service and the port service is running on.
   */
  protected abstract
  ImmutablePair<Map<String, String>, Integer> configure(String[] args, Configuration conf);


}
