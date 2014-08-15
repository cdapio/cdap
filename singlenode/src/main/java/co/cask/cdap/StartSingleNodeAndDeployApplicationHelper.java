/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Class for deploying to singlenode.
 */
public final class StartSingleNodeAndDeployApplicationHelper {

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

    System.out.println("Finished deploy, exit code: " + proc.waitFor());
  }

  private static final class StreamTailer extends Thread {
    private InputStream is;
    private String type;

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
        System.out.println("Tailing output failed for type: " + type + ". You may not see new output from this stream");
        ioe.printStackTrace();
        // DO NOTHING
      }
    }
  }
}
