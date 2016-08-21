/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.proto.ProgramType;
import org.apache.twill.internal.Constants;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimization for CDAP-7021.
 *
 * We cache appmaster and container jars created by twill the first time a program
 * of that type is run. However, if 100 programs are started at the same time, all 100 will still create those
 * jars since the cached jars won't be available at run start. So as an optimization, if a program is
 * launching and its cached jars are not available, and its not the first program of its type to be launched,
 * and its launching within 90 seconds of the first run, wait for the cache jars to become available.
 * Without this, we might actually miss schedule runs due to jar building taking too much time and resources.
 */
public class JarCacheTracker {
  public static final JarCacheTracker INSTANCE = new JarCacheTracker();
  private static final Logger LOG = LoggerFactory.getLogger(JarCacheTracker.class);
  private final Map<ProgramType, CacheInfo> programCaches;

  private JarCacheTracker() {
    programCaches = new ConcurrentHashMap<>();
    for (ProgramType programType : ProgramType.values()) {
      programCaches.put(programType, new CacheInfo());
    }
  }

  public void registerLaunch(File cacheDir, ProgramType programType) {
    CacheInfo cacheInfo = programCaches.get(programType);

    long launchTime = System.currentTimeMillis();
    if (cacheInfo.firstLaunchTime.compareAndSet(-1L, launchTime)) {
      // if this is the first program of this type to launch, no need to wait
      LOG.debug("First program of type {} launched at time {}", programType, launchTime);
      return;
    }

    // if we're more than 90 seconds past the first launch time, no need to waits
    long threshold = cacheInfo.firstLaunchTime.get() + TimeUnit.MILLISECONDS.convert(90, TimeUnit.SECONDS);
    if (System.currentTimeMillis() > threshold) {
      return;
    }

    // otherwise, wait until at most 90 seconds past the first launch time for the cache files to exist
    cacheInfo.waitForCacheFiles(cacheDir);
  }

  /**
   * Used to hold the first time a program of the type was launched and wait for the cache files to exist.
   */
  private class CacheInfo {
    private final AtomicLong firstLaunchTime = new AtomicLong(-1L);

    // synchronized here instead of at registerLaunch so that we only per program type.
    private synchronized void waitForCacheFiles(File cacheDir) {
      Path cachePath = cacheDir.getAbsoluteFile().toPath();
      Path containerDonePath = cachePath.resolve(Constants.Files.CONTAINER_JAR + ".done");
      Path appmasterDonePath = cachePath.resolve(Constants.Files.APP_MASTER_JAR + ".done");

      // check this in case we called this method before the files were available, were blocked on another thread,
      // and are now here
      long threshold = firstLaunchTime.get() + TimeUnit.MILLISECONDS.convert(90, TimeUnit.SECONDS);
      if (System.currentTimeMillis() > threshold ||
        (Files.exists(containerDonePath) && Files.exists(appmasterDonePath))) {
        return;
      }

      // otherwise, wait for at most threshold seconds for the cache files to exist
      try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
        WatchKey key;
        try {
          key = cachePath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
        } catch (Exception e) {
          LOG.error("Error watching {} for twill cache files to exist. Continuing on.", cacheDir, e);
          return;
        }

        // if the cache files were written while we were registering the watcher
        boolean containerJarExists = Files.exists(containerDonePath);
        boolean appmasterJarExists = Files.exists(appmasterDonePath);
        if (containerJarExists && appmasterJarExists) {
          LOG.debug("files exist, cancelling watch key.");
          key.cancel();
          return;
        }

        LOG.debug("Waiting for twill cache files at {}", cacheDir.getAbsoluteFile());
        while (!containerJarExists || !appmasterJarExists) {

          // wait at most threshold millis for some event to occur
          long maxWaitTime = threshold - System.currentTimeMillis();
          if (maxWaitTime < 0) {
            break;
          }
          try {
            key = watcher.poll(maxWaitTime, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            LOG.warn("Error while watching for cache jars. Continuing on.", e);
            return;
          }
          if (key == null) {
            LOG.debug("Waited past the threshold and cache jars are still not there. Continuing on.");
            return;
          }

          // examine all events on the directory
          for (WatchEvent<?> event : key.pollEvents()) {
            WatchEvent.Kind<?> kind = event.kind();

            // apparently overflow is possible regardless of what events were registered
            if (kind == StandardWatchEventKinds.OVERFLOW) {
              continue;
            }

            //noinspection unchecked
            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path filePath = cachePath.resolve(ev.context());
            LOG.debug("Got a watch event for file {}.", filePath);

            if (containerDonePath.equals(filePath)) {
              LOG.debug("container jar is cached.");
              containerJarExists = true;
            }
            if (appmasterDonePath.equals(filePath)) {
              LOG.debug("app master jar is cached.");
              appmasterJarExists = true;
            }
          }

          if (!key.reset()) {
            break;
          }
        }

        key.cancel();
      } catch (IOException e) {
        LOG.error("Unable to watch for jar cache files at {}. Continuing on.", cacheDir, e);
      }
    }
  }

}
