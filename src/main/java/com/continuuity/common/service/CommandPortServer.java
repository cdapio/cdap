package com.continuuity.common.service;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * CommandPortServer is simple TCP server that is embedded into {@link AbstractRegisteredService}
 * for managing some parts of service through command line. It's not multi-client. It supports
 * only one client to be connected at any given point in time. So, please do not try to use it
 * as service provider. You can attach listeners to the commands. When the command is received
 * it is dispatched to appropriate listener. By default, "help" has a default listener.
 * This works in conjunction with {@link AbstractRegisteredService} only for now.
 *
 * This is how it can be used :
 * <code>
 *   CommandPortServer server = new CommandPortServer();
 *   server.addListener("stats", "Provides stats", new CommandListener() {
 *    @Override
 *    public String act() {
 *      return whole_lot_of_stats;
 *    }
 *   });
 * </code>
 */
public class CommandPortServer {
  private static final Logger Log = LoggerFactory.getLogger(CommandPortServer.class);

  /**
   * Mappings of commands to their listener.
   */
  private final Map<String, CommandListener> listeners = Maps.newHashMap();

  /**
   * Help string being constructed as the services are registered.
   */
  private String helpString;

  /**
   * Server socket listening on command port.
   */
  private ServerSocket serverSocket;

  /**
   * Port the server is running on.
   */
  private int port;

  /**
   * Name of service that uses command port.
   */
  private final String serviceName;

  /**
   * Specifies whether command port should be running or no.
   */
  private boolean running = true;

  /**
   * Creates an instance of CommandPortServer.
   *
   * @param serviceName name of the service that uses command port.
   */
  public CommandPortServer(String serviceName) throws IOException {
    this.serviceName = serviceName;
    serverSocket = new ServerSocket(0, 0, InetAddress.getByName("localhost"));
    Log.info(String.format("Command server listening on %s",
      serverSocket.getLocalSocketAddress().toString()));
    port = serverSocket.getLocalPort();
    helpString = String.format("%s command port\n", serviceName);
  }

  /**
   * Port the command server is running on.
   *
   * @return port the command server is running on.
   */
  public int getPort() {
    return port;
  }

  /**
   * Adds a command listener. Listeners are invoke the command is received on command port.
   *
   * @param name  of the command
   * @param description description of command
   * @param listener listener to be executed when the command is received.
   */
  public void addListener(String name, String description, CommandListener listener) {
    listeners.put(name, listener);
    helpString = String.format("%s%10s : %30s\n", helpString, name, description);
  }

  /**
   * Blocking call that waits for connections and serves command one client at a time.
   * <p>
   *   Not facny multi-threaded server and don't expect it to be :-)
   * </p>
   *
   * @throws CommandPortException
   */
  public void serve() throws CommandPortException {
    boolean inSession = true;
    try {
      port = serverSocket.getLocalPort();
      while(running) {
        Socket socket = serverSocket.accept(); /** wait for connection */
        inSession = true;
        BufferedReader fromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        DataOutputStream toClient = new DataOutputStream(socket.getOutputStream());
        String command;
        while((command = fromClient.readLine()) != null) {
          if(command.isEmpty()) {
            break;
          }
          if(command.equals("help")) {
            toClient.writeBytes(helpString);
          } else if(command.equals("exit")) {
            break;
          } else if(listeners.containsKey(command)) {
            String message = listeners.get(command).act();
            toClient.writeBytes(message + "\n");
          }
        }
      }
    } catch (IOException e) {
      throw new CommandPortException(e.getMessage());
    }
  }

  /**
   * Stops the command port server.
   */
  public void stop() {
    running = false;
  }

  /**
   * Interface defining Command listeners.
   */
  public interface CommandListener {
    /**
     * Invoked when the command associated with this listener is received.
     * @return the string to returned to client in response to command.
     */
    public String act();
  }

  /**
   * Exception raised by {@link CommandPortServer} with proper reason.
   */
  @SuppressWarnings("serial")
  public class CommandPortException extends IOException {
    public CommandPortException(String reason) {
      super(reason);
    }
  }
}
