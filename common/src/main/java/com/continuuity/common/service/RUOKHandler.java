package com.continuuity.common.service;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Standard ruok command handler. Usage example:
 * {@code
 *     cmdService = CommandPortService.builder("my-service")
 *     .setPort(port)
 *     .addCommandHandler(RUOKHandler.COMMAND, RUOKHandler.DESCRIPTION, new RUOKHandler())
 *     .build();
 * }
 */
public class RUOKHandler implements CommandPortService.CommandHandler {
  public static final String COMMAND = "ruok";
  public static final String DESCRIPTION = "ruok";

  @Override
  public void handle(BufferedWriter respondWriter) throws IOException {
    respondWriter.write("imok");
    respondWriter.close();
  }
}
