package com.continuuity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

// TODO: looks like very generic: rename?
public class StartSingleNodeAndDeployApplicationHelper {

  public static final String ARG_APPFABRIC_SINGLENODE_DEPLOY_COMMAND =
    "appfabric.singlenode.deploy.command";

  public static void main(String[] args) throws IOException, InterruptedException {
    // Starting Singlenode
    SingleNodeMain.main(args);

    String deployCmd =
      System.getProperty(ARG_APPFABRIC_SINGLENODE_DEPLOY_COMMAND);
    System.out.println("Deploying application...");
    Process proc = Runtime.getRuntime().exec(deployCmd);

    // any error message?
    StreamTailer errorTailer = new StreamTailer(proc.getErrorStream(), "ERROR");

    // any output?
    StreamTailer outputTailer = new StreamTailer(proc.getInputStream(), "OUTPUT");

    errorTailer.start();
    outputTailer.start();

    int exitVal = proc.waitFor();
    System.out.println("Finished deploy, exit code: " + exitVal);
  }

  private static class StreamTailer extends Thread {
    InputStream is;
    String type;

    StreamTailer(InputStream is, String type) {
      this.is = is;
      this.type = type;
    }

    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          System.out.println(type + " > " + line);
        }

      } catch (IOException ioe) {
        ioe.printStackTrace();
        // DO NOTHING
      }
    }
  }
}
