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

package co.cask.cdap.runtime.spi.ssh;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This interface represents an SSH session, which allow performing remote ssh commands and scp.
 */
public interface SSHSession extends AutoCloseable {

  /**
   * Returns the remote host and port that this session is connected to.
   *
   * @return a {@link InetSocketAddress} containing the target host and port of this session
   */
  InetSocketAddress getAddress();

  /**
   * Returns the remote user name that this session used to connect to the remote host.
   *
   * @return the remote user name
   */
  String getUsername();

  /**
   * Executes a sequence of commands on the remote host.
   *
   * @param commands the commands to execute
   * @return the command result
   * @throws IOException if failed to execute command remotely
   */
  default SSHProcess execute(String... commands) throws IOException {
    return execute(Arrays.asList(commands));
  }

  /**
   * Executes a sequence of commands on the remote host.
   *
   * @param commands the commands to execute
   * @return the command result
   * @throws IOException if failed to execute command remotely
   */
  SSHProcess execute(List<String> commands) throws IOException;

  /**
   * Executes a sequence of commands on the remote host and block until execution completed.
   *
   * @param commands the commands to execute
   * @return the output to stdout by the commands
   * @throws IOException if failed to execute command or command exit with non-zero values.
   */
  default String executeAndWait(String... commands) throws IOException {
    return executeAndWait(Arrays.asList(commands));
  }

  /**
   * Executes a sequence of commands on the remote host and block until execution completed.
   *
   * @param commands the commands to execute
   * @return the output to stdout by the commands
   * @throws IOException if failed to execute command or command exit with non-zero values.
   */
  String executeAndWait(List<String> commands) throws IOException;

  /**
   * Copies a local file to the given target path
   *
   * @param sourceFile source file
   * @param targetPath the target path to copy to
   * @throws IOException if
   */
  void copy(Path sourceFile, String targetPath) throws IOException;

  /**
   * Copies content to remote host.
   *
   * @param input {@link InputStream} for the source content
   * @param targetPath target path. If the path is an existing directory, file with the {@code targetName} will
   *                   be created under the given path. If the path doesn't exist or is an existing file,
   *                   content will be written/overwritten to the given path
   * @param targetName file name in the {@code targetPath} if {@code targetPath} is a directory
   * @param size size of the content
   * @param permission permission of the target file
   * @param lastAccessTime the optional file last access time in milliseconds.
   *                       Both this and the {@code lastModifiedTime} should not be {@code null} to have time to be set
   * @param lastModifiedTime the optional file last modified time in milliseconds.
   *                         Both this and the {@code lastAccessTime} should not be {@code null} to have time to be set
   * @throws IOException if failed to copy the content
   */
  void copy(InputStream input, String targetPath, String targetName, long size, int permission,
            @Nullable Long lastAccessTime, @Nullable Long lastModifiedTime) throws IOException;

  /**
   * Close this SSH session.
   */
  @Override
  void close();
}
