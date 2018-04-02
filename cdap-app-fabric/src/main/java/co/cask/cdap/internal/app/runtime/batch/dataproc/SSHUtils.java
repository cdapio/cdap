/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 *
 */
public class SSHUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SSHUtils.class);

  public static void scp(SSHConfig SSHConfig, String localFile, String remoteDir) throws JSchException, IOException {
    // https://medium.com/@ldclakmal/scp-with-java-b7b7dbcdbc85
    com.jcraft.jsch.Session session =
      createSession(SSHConfig.user, SSHConfig.host, SSHConfig.port, SSHConfig.privateKeyFile, SSHConfig.passphrase);

    LOG.info("Starting SCP from {} to {}", localFile, remoteDir);
    copyLocalToRemote(session, localFile, remoteDir);
    LOG.info("Finished SCP from {} to {}", localFile, remoteDir);
  }

  private static com.jcraft.jsch.Session createSession(String user, String host, int port, String keyFilePath, String keyPassword) throws JSchException, UnsupportedEncodingException {
    JSch jsch = new JSch();

//    if (keyFilePath != null) {
//      if (keyPassword != null) {
//        jsch.addIdentity(keyFilePath, keyPassword);
//      } else {
//        jsch.addIdentity(keyFilePath);
//        jsch.addIdentity("");
//      }
//    }

    jsch.addIdentity("name", keyFilePath.getBytes("UTF-8"), null, null);


    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");

    com.jcraft.jsch.Session session = jsch.getSession(user, host, port);
    session.setConfig(config);
    session.connect();

    return session;
  }

  private static void copyLocalToRemote(com.jcraft.jsch.Session session, String from, String to) throws JSchException, IOException {
    boolean ptimestamp = true;

    // exec 'scp -t rfile' remotely
    String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + to;
    Channel channel = session.openChannel("exec");
    ((ChannelExec) channel).setCommand(command);

    // get I/O streams for remote scp
    OutputStream out = channel.getOutputStream();
    InputStream in = channel.getInputStream();

    channel.connect();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }

    File _lfile = new File(from);

    if (ptimestamp) {
      command = "T" + (_lfile.lastModified() / 1000) + " 0";
      // The access time should be sent here,
      // but it is not accessible with JavaAPI ;-<
      command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
      out.write(command.getBytes());
      out.flush();
      if (checkAck(in) != 0) {
        throw new IllegalStateException("Ack failed.");
      }
    }

    // send "C0644 filesize filename", where filename should not include '/'
    long filesize = _lfile.length();
    command = "C0644 " + filesize + " ";
    if (from.lastIndexOf('/') > 0) {
      command += from.substring(from.lastIndexOf('/') + 1);
    } else {
      command += from;
    }

    command += "\n";
    out.write(command.getBytes());
    out.flush();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }

    // send a content of lfile
    FileInputStream fis = new FileInputStream(from);
    byte[] buf = new byte[1024];
    while (true) {
      int len = fis.read(buf, 0, buf.length);
      if (len <= 0) break;
      out.write(buf, 0, len); //out.flush();
    }

    // send '\0'
    buf[0] = 0;
    out.write(buf, 0, 1);
    out.flush();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }
    out.close();

    if (fis != null) {
      fis.close();
    }

    channel.disconnect();
    session.disconnect();
  }

  private static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //         -1
    if (b == 0) return b;
    if (b == -1) return b;

    if (b == 1 || b == 2) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      }
      while (c != '\n');
      if (b == 1) { // error
        System.out.print(sb.toString());
      }
      if (b == 2) { // fatal error
        System.out.print(sb.toString());
      }
    }
    return b;
  }

}
