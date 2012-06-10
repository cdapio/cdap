package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.io.IOException;

/**
 * This abstract class makes it easy to build a registered server.
 * <p>
 *   Server provides an ability to register the server with the central
 *   server discovery system and also provides a command port that can be accessed
 *   locally to investigate the server.
 * </p>
 */
public abstract class AbstractRegisteredServer {
  private static final Logger Log = (Logger)LoggerFactory.getLogger(AbstractRegisteredServer.class);

  /**
   * Service registration client.
   */
  private ServiceDiscoveryClient client;

  /**
   * Name of the server
   */
  private String server = "NA";

  /**
   * Command port cmdPortServer associated with the service.
   */
  private CommandPortServer cmdPortServer;

  /**
   * Server thread returned from starting of the service. The service thread is actually managed
   * by the AbstractRegisteredServer.
   */
  private Thread serverThread;

  /**
   * Set server name
   *
   * @param server name
   */
  public void setServerName(String server) {
    this.server = server;
  }

  /**
   * Returns name of the service
   * @return name of the service.
   */
  public String getServerName() {
    return this.server;
  }

  /**
   * Adds a command listener to the command port cmdPortServer.
   * <p>
   *   NOTE: If the same command is registered twice the latest one wins.
   * </p>
   * @param command to be registered.
   * @param description of the command that is being registered.
   * @param listener to be associated with command, that is invoked when command port sees that.
   */
  public void addCommandListener(String command, String description, CommandPortServer.CommandListener listener) {
    cmdPortServer.addListener(command, description, listener);
  }

  /**
   * Starts the service
   *
   * @param args arguments for the service
   * @param conf instance of configuration object.
   * @throws ServerException
   */
  public final void start(String[] args, CConfiguration conf) throws ServerException {
    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
    Preconditions.checkNotNull(zkEnsemble);

    try {
      cmdPortServer = new CommandPortServer(server);

      addCommandListener("stop", "Stops the service", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          stop(true);
          return "Done";
        }
      });

      addCommandListener("log-trace", "Change service level log to trace", new CommandPortServer.CommandListener(){
        @Override
        public String act() {
          Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
          root.setLevel(Level.DEBUG);
          return "WARNING: Please note that this might fill out the disk space on host. Set log level to debug";
        }
      });

      addCommandListener("log-debug", "Change service level log to debug", new CommandPortServer.CommandListener(){
        @Override
        public String act() {
          Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
          root.setLevel(Level.DEBUG);
          return "Set log level to debug";
        }
      });

      addCommandListener("log-error", "Change service level log to error.", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
          root.setLevel(Level.ERROR);
          return "Set log level to error";
        }
      });

      addCommandListener("log-warn", "Change service level log to warn.", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
          root.setLevel(Level.WARN);
          return "Set log level to warn";
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

      ImmutablePair<ServiceDiscoveryClient.ServicePayload, Integer>
        serviceArgs = configure(args, conf);
      if(serviceArgs == null) {
        throw new ServerException("configuration of service failed.");
      }

      client = new ServiceDiscoveryClient(zkEnsemble);
      client.register(server, serviceArgs.getSecond().intValue(), serviceArgs.getFirst());

      serverThread = start();
      if(serverThread == null) {
        throw new ServerException("Thread returned from start is null");
      }
      serverThread.start();
      cmdPortServer.serve();
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to register the cmdPortServer with discovery service, shutting down. Reason {}", e.getMessage());
      stop(true);
      throw new ServerException("Unable to register the cmdPortServer with discovery service");
    } catch (CommandPortServer.CommandPortException e) {
      Log.warn("Error starting the command port service. Service not started. Reason : {}", e.getMessage());
      throw new ServerException("Could not start command port service. Reason : " + e.getMessage());
    } catch (IOException e) {
      Log.error("Error starting the command port service. Reason : {}", e.getMessage());
      throw new ServerException("Could not start command port service. Reason : " + e.getMessage());
    }
  }

  /**
   * Stops the service.
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public final void stop(boolean now) {
    try {
      client.close();
      cmdPortServer.stop();
    } catch (IOException e) {
      Log.warn("Issue while closing the service discovery client. Reason : {}", e.getMessage());
    }
    stop();
  }

  /**
   * Extending class should implement this API. Should do everything to start a service in the Thread.
   * @return Thread instance that will be managed by the service.
   */
  protected abstract Thread start();

  /**
   * Should be implemented by the class extending {@link AbstractRegisteredServer} to stop the service.
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
  protected abstract ImmutablePair<ServiceDiscoveryClient.ServicePayload, Integer>
    configure(String[] args, Configuration conf);


}
