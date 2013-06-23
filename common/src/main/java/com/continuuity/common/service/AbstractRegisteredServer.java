package com.continuuity.common.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * This abstract class makes it easy to build a registered server.
 * <p>
 *   Server provides an ability to register the server with the central
 *   server discovery system and also provides a command port that can be
 *   accessed locally to investigate the server.
 * </p>
 */
public abstract class AbstractRegisteredServer {
  private static final Logger Log = LoggerFactory.getLogger
    (AbstractRegisteredServer.class);

  /**
   * Name of the server
   */
  private String server = "NA";

  /**
   * Command port cmdPortServer associated with the service.
   */
  private CommandPortServer cmdPortServer;


  private Cancellable discoveryServiceCancellable;

  /**
   * Indicates whether the registered server has started or no.
   */
  private volatile boolean running = false;

  /**
   * Server thread returned from starting of the service. The service thread is
   * actually managedby the AbstractRegisteredServer.
   */
  private Thread serverThread;

  /**
   * Defines the time start will wait till it's started. if server is not
   * started even after this time, then there is an issue and will throw a
   * ServerException.
   */
  private static final long START_WAIT_TIME = 5 * 1000;

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
   * @param listener to be associated with command, that is invoked when
   *                 command port sees that.
   */
  public void addCommandListener(String command, String description,
                                 CommandPortServer.CommandListener listener) {
    cmdPortServer.addListener(command, description, listener);
  }

  /**
   * Iterate through all the configured loggers and changes the level
   * to the level specified by <code>level</code>.
   *
   * @param level The level to be set on logger.
   */
  public static void setLoggingLevel(Level level) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    for(ch.qos.logback.classic.Logger log : lc.getLoggerList()) {
      log.setLevel(level);
    }
  }
  /**
   * Starts the service
   *
   * @param args arguments for the service
   * @param conf instance of configuration object.
   * @throws ServerException
   */
  public final void start(String[] args, CConfiguration conf)
    throws ServerException {
    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE,
                                 Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
    Log.info("AbstractRegisteredServer using ensemble {}", zkEnsemble);

    Preconditions.checkNotNull(zkEnsemble);

    try {
      cmdPortServer = new CommandPortServer(server);

      // Add some commands to the cmd server
      addCommandListener("stop", "Stops the service", new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          stop(true);
          return "Done";
        }
      });

      addCommandListener("log-trace",
                         "Change service level log to trace",
                         new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          setLoggingLevel(Level.TRACE);
          return "WARNING: Please note that this might fill out the disk " +
            "space on host. Set log level to debug";
        }
      });

      addCommandListener("log-debug", "Change service level log to debug",
                         new CommandPortServer.CommandListener(){
        @Override
        public String act() {
          setLoggingLevel(Level.DEBUG);
          return "Set log level to debug";
        }
      });

      addCommandListener("log-error", "Change service level log to error.",
                         new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          setLoggingLevel(Level.ERROR);
          return "Set log level to error";
        }
      });

      addCommandListener("log-warn", "Change service level log to warn.",
                         new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          setLoggingLevel(Level.WARN);
          return "Set log level to warn";
        }
      });

      addCommandListener("ruok", "Checks if service is up",
                         new CommandPortServer.CommandListener() {
        @Override
        public String act() {
          if(ruok()) {
            return "imok";
          }
          return "i am in trouble";
        }
      });

      final RegisteredServerInfo serverArgs = configure(args, conf); // Declaring final to access in inner class
      if(serverArgs == null) {
        throw new ServerException("configuration of service failed.");
      }

      ZKClientService zkClient = ZKClientServices.delegate(
        ZKClients.retryOnFailure(
          ZKClients.reWatchOnExpire(
            ZKClientService.Builder.of(zkEnsemble).build()),
          RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));
      zkClient.startAndWait();

      DiscoveryService discoveryService = new ZKDiscoveryService(zkClient);

      InetAddress address = InetAddress.getByName(serverArgs.getAddress());
      if (address.isAnyLocalAddress()) {
        address = InetAddress.getLocalHost();
      }
      final InetAddress finalAddress = address;
      discoveryServiceCancellable = discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return server;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return new InetSocketAddress(finalAddress, serverArgs.getPort());
        }
      });

      serverThread = start();
      if(serverThread == null) {
        throw new ServerException("Thread returned from start is null");
      }

      serverThread.setName(getServerName() + "-Thread");
      serverThread.start();

      // We wait till we either find that the service has started or reaches
      // timeout.
      StopWatch watch = new StopWatch();
      watch.start();
      while(watch.getTime() < START_WAIT_TIME) {
        if(ruok()) {
          running = true;
          break;
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if(!running) {
        throw new ServerException("Service not started even after waiting for "
                                    + START_WAIT_TIME + "ms.");
      }

      // If command port has been enabled, then we block else we destroy
      // the command port object.
      if(conf.getBoolean(Constants.CFG_COMMAND_PORT_ENABLED,
                         Constants.DEFAULT_COMMAND_PORT_ENABLED)) {
        cmdPortServer.serve();
      } else {
        cmdPortServer = null;
      }

    } catch (CommandPortServer.CommandPortException e) {
      Log.warn("Error starting the command port service. Service not " +
                 "started. Reason : {}", e.getMessage());
      stop(true);
      throw new ServerException("Could not start command port service. " +
                                  "Reason : " + e.getMessage());
    } catch (IOException e) {
      Log.error("Error starting the command port service. Reason : {}",
                e.getMessage());
      stop(true);
      throw new ServerException("Could not start command port service. " +
                                  "Reason : " + e.getMessage());
    }
  }

  /**
   * Stops the service.
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   */
  public final void stop(boolean now) {
    // Stop the services started by implementation class.
    stop();

    if (discoveryServiceCancellable != null){
      discoveryServiceCancellable.cancel();
    }

    if(cmdPortServer != null) {
      cmdPortServer.stop();
    }

  }

  /**
   * Returns the <code>InetAddress</code> on which the server can be started.
   *
   * <p> A preferredAddress specifies the address to used for create
   * InetAddress, if it's not present then localhost is returned.</p>
   *
   * @param preferredAddress address that is preferred to start the server on.
   * @return an instance of {@link InetAddress}
   * @throws UnknownHostException
   */
  protected final InetAddress getServerInetAddress(String preferredAddress)
    throws UnknownHostException {
    InetAddress listenAddress = null;
    if(preferredAddress == null) {
      listenAddress = InetAddress.getLocalHost();
    } else {
      listenAddress = InetAddress.getByName(preferredAddress);
    }
    return listenAddress;
  }

  /**
   * Returns the max read buffer size for thrift server.
   *
   * @return max read buffer size.
   */
  public long getMaxReadBuffer(CConfiguration configuration) {
    String maxReadBufferStr = configuration.get(Constants.CFG_MAX_READ_BUFFER);
    if(maxReadBufferStr == null || maxReadBufferStr.isEmpty()) {
      return Constants.DEFAULT_MAX_READ_BUFFER;
    }
    long maxReadBuffer = Integer.valueOf(maxReadBufferStr);
    return maxReadBuffer;
  }

  /**
   * Extending class should implement this API. Should do everything to start
   * a service in the Thread.
   * @return Thread instance that will be managed by the service.
   */
  protected abstract Thread start();

  /**
   * Should be implemented by the class extending
   * {@link AbstractRegisteredServer} to stop the service.
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
   * @return Pair of args for registering the service and the port service
   * is running on.
   */
  protected abstract RegisteredServerInfo configure(String[] args,
                                                    CConfiguration conf);


}
