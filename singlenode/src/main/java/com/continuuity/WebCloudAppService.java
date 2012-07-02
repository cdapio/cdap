package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.Server;
import com.continuuity.common.service.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * WebCloudAppService is a basic Server wrapper that launches node.js and our
 * webapp main.js file. It then basically sits there waiting, doing nothing.
 *
 * All output is sent to our Logging service.
 */
public class WebCloudAppService implements Server {

  /**
   * This is our Logger instance
   */
  private static final Logger logger =
    LoggerFactory.getLogger(WebCloudAppService.class);

  /**
   * This is the external process that will wrap the web app
   */
  Process webAppProcess;

  @Override
  public void start(String[] args, CConfiguration conf) throws ServerException {

    // Create a new ProcessBuilder
    ProcessBuilder builder =
      new ProcessBuilder("node", conf.get("webapp.main"));

    // Re-direct all our stderr to stdout
    builder.redirectErrorStream(true);

    try {

      // Now try to launch the app
      logger.info("Launching BigFlow Monitoring Web Application");
      webAppProcess = builder.start();

      // Keep running..
      final Process localProcess = webAppProcess;
      final CountDownLatch latch = new CountDownLatch(1);
      new Thread() {
        @Override
        public void run() {

          try {

            // Read our output
            InputStream is = localProcess.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // And print it to our logger output
            String line;
            while ((line = br.readLine()) != null) {
              logger.debug(line);
            }
            latch.countDown();
          } catch (IOException ie) {
            logger.error(ie.getMessage());
          }
        }
      }.start();

      try {
        latch.await(2, TimeUnit.SECONDS);
        if(latch.getCount() != 0) {
          throw new ServerException("There was a problem starting the nodejs server.");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new ServerException(e.getMessage());
    }
  }

  /**
   * Shut our running service. Currently all this does is call
   * destroy on the wrapper Process object.
   *
   * @param now true specifies non-graceful shutdown; false otherwise.
   *
   * @throws ServerException
   */
  @Override
  public void stop(boolean now) throws ServerException {
    webAppProcess.destroy();
  }

} // end of WebCloudAppService class
