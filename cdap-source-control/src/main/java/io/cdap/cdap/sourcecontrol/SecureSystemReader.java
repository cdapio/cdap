/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.Collections;
import java.util.Set;
import org.eclipse.jgit.lib.Config;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.SystemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SystemReader} that disables features and hides properties to ensure
 * user code isn't executed accidentally.
 */
public class SecureSystemReader extends SystemReader {

  private static final String HOSTNAME = "cdap.io";
  private static final Logger LOG = LoggerFactory.getLogger(
      SecureSystemReader.class);
  // ALLOWED_SYSTEM_PROPERTIES stores properties that are essential for git to
  // work.
  private static final Set<String> REQUIRED_SYSTEM_PROPERTIES = Collections.singleton(
      "os.name");

  private final Config gitConfig;
  private final SystemReader delegate;

  private SecureSystemReader(SystemReader delegate) {
    this.delegate = delegate;
    this.gitConfig = new Config();
    this.gitConfig.setString("core", null, "hooksPath", "/dev/null");
  }

  @Override
  public String getHostname() {
    return HOSTNAME;
  }

  @Override
  public String getenv(String s) {
    return null;
  }

  @Override
  public String getProperty(String s) {
    // Restrict access to environment variables which are used by Git.
    // Environment variables such as GIT_DIR can be used to make jgit read
    // configuration from different directories on the file system.
    // Git uses default values of most properties that can be configured using
    // environment variables. Values which are required are read through the
    // delegate.
    if (REQUIRED_SYSTEM_PROPERTIES.contains(s)) {
      return delegate.getProperty(s);
    }
    return null;
  }

  @Override
  public FileBasedConfig openUserConfig(Config config, FS fs) {
    return new EmptyConfig(gitConfig, null, fs);
  }

  @Override
  public FileBasedConfig openSystemConfig(Config config, FS fs) {
    return new EmptyConfig(gitConfig, null, fs);
  }

  @Override
  public FileBasedConfig openJGitConfig(Config config, FS fs) {
    return new EmptyConfig(gitConfig, null, fs);
  }

  @Override
  public long getCurrentTime() {
    return delegate.getCurrentTime();
  }

  @Override
  public int getTimezone(long l) {
    return delegate.getTimezone(l);
  }

  /**
   * Sets a {@link SecureSystemReader} as the {@link SystemReader} used by Jgit.
   */
  public static void setAsSystemReader() {
    SystemReader currentSystemReader = SystemReader.getInstance();
    if (currentSystemReader instanceof SecureSystemReader) {
      return;
    }
    LOG.info("Setting SecureSystemReader for Git.");
    SystemReader.setInstance(new SecureSystemReader(currentSystemReader));
  }

  @VisibleForTesting
  SystemReader getDelegate() {
    return delegate;
  }

  /**
   * A {@link FileBasedConfig} that can't be updated.
   */
  private static class EmptyConfig extends FileBasedConfig {

    EmptyConfig(Config base, File cfgLocation, FS fs) {
      super(base, cfgLocation, fs);
    }

    @Override
    public void load() {
      // no-op
    }

    @Override
    public void save() {
      // no-op
    }

    @Override
    public boolean isOutdated() {
      return false;
    }

    @Override
    public String toString() {
      return "EmptyConfig";
    }
  }
}
