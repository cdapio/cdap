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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The clean up service that cleans up cloned repositories periodically.
 */
public class RepositoryCleanupService extends AbstractScheduledService {
  private final long cleanUpInterval;
  private final long ttlInSeconds;
  private ScheduledExecutorService executor;
  private final Path repositoryCloneDirectory;
  private static final Logger LOG = LoggerFactory.getLogger(RepositoryCleanupService.class);

  @Inject
  RepositoryCleanupService(CConfiguration cConf) {
    this.cleanUpInterval = cConf.getLong(Constants.SourceControlManagement.REPOSITORY_CLEANUP_INTERVAL_SECONDS);
    this.ttlInSeconds = cConf.getLong(Constants.SourceControlManagement.REPOSITORY_TTL_SECONDS);
    this.repositoryCloneDirectory =
        Paths.get(cConf.get(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH));
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("git-repository-cleanup"));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    deleteExpiredRepository();
  }

  @Override
  protected Scheduler scheduler() {
    // Try right away if there's anything to cleanup
    return Scheduler.newFixedRateSchedule(1, cleanUpInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /**
   * The repositories are cloned under /clone_root/namespace/namespace_name/ path.
   * see {@link RepositoryManager}
   * @return List of repository directories
   */
  private List<File> getRepositoryDirectories() {
    Path rootDirectoryPath = repositoryCloneDirectory.resolve(SourceControlConfig.cloneDirectoryPrefix);
    List<File> namespaceDirectories = DirUtils.listFiles(rootDirectoryPath.toFile(), File::isDirectory);
    return namespaceDirectories.stream().map(
        d -> DirUtils.listFiles(d, File::isDirectory)
    ).flatMap(List::stream).collect(Collectors.toList());
  }

  private void deleteExpiredRepository() {
    try {
      List<File> repositoryDirs = getRepositoryDirectories();
      long currentTimeInSeconds = System.currentTimeMillis() / 1000;
      for (File repositoryDir : repositoryDirs) {
        FileTime createTimeStamp = FileUtils.getFileCreationTime(repositoryDir.toPath());
        if (createTimeStamp != null && (currentTimeInSeconds - createTimeStamp.toMillis() / 1000) > ttlInSeconds){
          DirUtils.deleteDirectoryContents(repositoryDir);
          LOG.debug("Deleted expired repository {}", repositoryDir);
        }
      }
    }
    catch (IOException e) {
      LOG.error("Failed to run repository cleanup", e);
    }
  }
}
