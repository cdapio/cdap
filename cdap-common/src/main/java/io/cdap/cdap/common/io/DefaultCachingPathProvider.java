/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.hash.Hashing;
import io.cdap.cdap.common.utils.DirUtils;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A {@link CachingPathProvider} that generates local path based on location name and last modified time, which assumes
 * that a location won't change more frequent that the smallest file time granularity that the FS can support.
 * It also manages unused cache files based on last access time.
 */
public class DefaultCachingPathProvider implements CachingPathProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCachingPathProvider.class);

  private final LoadingCache<CacheKey, Path> cache;
  private final Path cacheDir;

  public DefaultCachingPathProvider(Path cacheDir, long cacheExpiry, TimeUnit cacheExpiryUnit) {
    this.cacheDir = cacheDir;
    this.cache = CacheBuilder.newBuilder()
      .expireAfterAccess(cacheExpiry, cacheExpiryUnit)
      .removalListener((RemovalListener<CacheKey, Path>) notification -> {
        Path path = notification.getValue();
        if (path == null) {
          return;
        }
        try {
          Files.deleteIfExists(path);
          LOG.debug("Removed cache file {}", path);
        } catch (IOException e) {
          LOG.warn("Failed to delete cache file {}. An unused file may left behind.", path, e);
        }
        // Try to delete the parent directory. We ignore the failure as it can happen if the directory is not empty.
        try {
          if (Files.deleteIfExists(path.getParent())) {
            LOG.debug("Removed cache directory {}", path.getParent());
          }
        } catch (IOException e) {
          LOG.trace("Failed to delete directory {}", path.getParent(), e);
        }
      })
      .build(new CacheLoader<CacheKey, Path>() {
        @Override
        public Path load(CacheKey key) {
          return getCachePath(key.getFileName(), key.getLastModified());
        }
      });

    populateCache(cacheDir, cache);
  }

  @Override
  public Optional<Path> apply(Location location) {
    try {
      return Optional.of(cache.get(new CacheKey(getCacheName(location), location.lastModified())));
    } catch (Exception e) {
      LOG.warn("Failed to get cache path for location {}", location, e);
      return Optional.empty();
    }
  }

  @VisibleForTesting
  void clearCache() {
    cache.invalidateAll();
  }

  @VisibleForTesting
  void clearCache(String fileName, long lastModified) {
    cache.invalidate(new CacheKey(fileName, lastModified));
  }

  String getCacheName(Location location) {
    return Hashing.md5().hashString(location.toURI().getPath()).toString() + "-" + location.getName();
  }

  @VisibleForTesting
  Path getCachePath(String fileName, long lastModified) {
    Path dir = cacheDir.resolve(fileName);
    return dir.resolve(lastModified + "-" + fileName);
  }

  @VisibleForTesting
  Collection<Path> getCacheEntries() {
    return new ArrayList<>(cache.asMap().values());
  }

  /**
   * Populates the cache based on the cache directory content.
   */
  private static void populateCache(Path cacheDir, LoadingCache<CacheKey, Path> cache) {
    for (File dir : DirUtils.listFiles(cacheDir.toFile(), File::isDirectory)) {
      String name = dir.getName();

      // List out all cache files, each prefixed with timestamp
      for (File file : DirUtils.listFiles(dir, f -> f.getName().endsWith(name))) {
        String fileName = file.getName();
        int idx = fileName.indexOf("-");
        if (idx <= 0) {
          continue;
        }
        long lastModified = Long.parseLong(fileName.substring(0, idx));

        cache.put(new CacheKey(fileName, lastModified), file.toPath());
        LOG.debug("Populate cache with {}", file);
      }
    }
  }

  /**
   * Cache key that contains the location file name and the last modified time.
   */
  private static final class CacheKey {
    private final String fileName;
    private final long lastModified;

    private CacheKey(String fileName, long lastModified) {
      this.fileName = fileName;
      this.lastModified = lastModified;
    }

    String getFileName() {
      return fileName;
    }

    long getLastModified() {
      return lastModified;
    }
  }
}
