package com.continuuity.common.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineReader;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * This class acts as a simple TCP server that accepts textual command and produce textual response.
 * The serving loop is single thread and can only serve one client at a time. {@link CommandHandler}s
 * binded to commands are invoked from the serving thread and is expected to return promptly to not
 * blocking the serving thread.
 *
 * Sample usage:
 * <pre>
 *   CommandPortService service = CommandPortService.builder("myservice")
 *                                                  .addCommandHandler("ruok", "Are you okay?", ruokHandler)
 *                                                  .build();
 *   service.startAndWait();
 * </pre>
 *
 * To stop the service, invoke {@link #stop()} or {@link #stopAndWait()}.
 */
public final class CommandPortService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(CommandPortService.class);

  /**
   * Stores binding from command to {@link CommandHandler}.
   */
  private final Map<String, CommandHandler> handlers;

  /**
   * The server socket for accepting incoming requests.
   */
  private ServerSocket serverSocket;

  /**
   * Port that the server socket binded to. It will only be set after the server socket is binded.
   */
  private int port;

  /**
   * Returns a {@link Builder} to build instance of this class.
   * @param serviceName Name of the service name to build
   * @return A {@link Builder}.
   */
  public static Builder builder(String serviceName) {
    return new Builder(serviceName);
  }

  private CommandPortService(int port, Map<String, CommandHandler> handlers) {
    this.port = port;
    this.handlers = handlers;
  }

  @Override
  protected void startUp() throws Exception {
    serverSocket = new ServerSocket(port, 0, InetAddress.getByName("localhost"));
    port = serverSocket.getLocalPort();
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Running commandPortService at localhost:" + port);
    serve();
  }

  @Override
  protected void shutDown() throws Exception {
    // The serverSocket would never be null if this method is called (guaranteed by AbstractExecutionThreadService).
    serverSocket.close();
  }

  @Override
  protected void triggerShutdown() {
    // The serverSocket would never be null if this method is called (guaranteed by AbstractExecutionThreadService).
    try {
      if (serverSocket.isBound()) {
        serverSocket.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns the port that the server is binded to.
   *
   * @return An int represent the port number.
   */
  public int getPort() {
    Preconditions.checkState(isRunning());
    return port;
  }

  /**
   * Starts accepting incoming request. This method would block until this service is stopped.
   *
   * @throws IOException If any I/O error occurs on the socket connection.
   */
  private void serve() throws IOException {
    while (isRunning()) {
      try {
        Socket socket = serverSocket.accept();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
        try {
          // Read the client command and dispatch
          String command = new LineReader(new InputStreamReader(socket.getInputStream(), "UTF-8")).readLine();
          CommandHandler handler = handlers.get(command);

          if (handler != null) {
            try {
              handler.handle(writer);
            } catch (Throwable t) {
              LOG.error(String.format("Exception thrown from CommandHandler for command %s", command), t);
            }
          }
        } finally {
          writer.flush();
          socket.close();
        }
      } catch (Throwable th) {
        // NOTE: catch any exception to keep the main service running
        // Trigger by serverSocket.close() through the call from stop().
        LOG.debug(th.getMessage(), th);
      }
    }
  }

  /**
   * Builder for creating {@link CommandPortService}.
   */
  public static final class Builder {
    private final ImmutableMap.Builder<String, CommandHandler> handlerBuilder;
    private final StringBuilder helpStringBuilder;
    private boolean hasHelp;
    private int port = 0;

    /**
     * Creates a builder for the give name.
     *
     * @param serviceName Name of the {@link CommandPortService} to build.
     */
    private Builder(String serviceName) {
      handlerBuilder = ImmutableMap.builder();
      helpStringBuilder = new StringBuilder(String.format("Help for %s command port service", serviceName));
    }

    /**
     * Adds a {@link CommandHandler} for a given command.
     *
     * @param command Name of the command handled by the handler.
     * @param desc A human readable description about the command.
     * @param handler The {@link CommandHandler} to invoke when the given command is received.
     * @return This {@link Builder}.
     */
    public Builder addCommandHandler(String command, String desc, CommandHandler handler) {
      hasHelp = "help".equals(command);
      handlerBuilder.put(command, handler);
      helpStringBuilder.append("\n").append(String.format("  %s : %s", command, desc));
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * Builds the {@link CommandPortService}.
     *
     * @return A {@link CommandPortService}.
     */
    public CommandPortService build() {
      if (!hasHelp) {
        final String helpString = helpStringBuilder.toString();
        handlerBuilder.put("help", new CommandHandler() {
          @Override
          public void handle(BufferedWriter respondWriter) throws IOException {
            respondWriter.write(helpString);
            respondWriter.newLine();
          }
        });
      }

      return new CommandPortService(port, handlerBuilder.build());
    }
  }

  /**
   * Interface for defining handler to react to a command.
   */
  public interface CommandHandler {

    /**
     * Invoked when the command that tied to this handler has been received
     * by the {@link CommandPortService}.
     *
     * @param respondWriter The {@link Writer} for writing response back to client
     * @throws IOException If I/O errors occurs when writing to the given writer.
     */
    void handle(BufferedWriter respondWriter) throws IOException;
  }
}
