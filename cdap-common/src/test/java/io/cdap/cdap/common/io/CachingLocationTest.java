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

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link CachingLocationFactory} and {@link CachingLocation}.
 */
public class CachingLocationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCache() throws IOException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    LocationFactory lf = new CachingLocationFactory(new LocalLocationFactory(TEMP_FOLDER.newFolder()),
                                                    l -> Optional.of(cachePath.resolve(l.getName() + ".cache")));
    Location location = lf.create("test");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Check if the cache file is there
    Path cachedFile = cachePath.resolve(location.getName() + ".cache");
    Assert.assertTrue(Files.exists(cachedFile));
    try (InputStream is = Files.newInputStream(cachedFile)) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testNoCache() throws IOException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    LocationFactory lf = new CachingLocationFactory(new LocalLocationFactory(TEMP_FOLDER.newFolder()),
                                                    l -> Optional.empty());
    Location location = lf.create("test");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // The caching directory should be empty
    Path cachedFile = cachePath.resolve(location.getName() + ".cache");
    Assert.assertFalse(Files.exists(cachedFile));
  }

  @Test
  public void testCachingPathProvider() throws IOException, InterruptedException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    DefaultCachingPathProvider cacheProvider = new DefaultCachingPathProvider(cachePath, 1, TimeUnit.HOURS);
    LocationFactory lf = new CachingLocationFactory(new LocalLocationFactory(TEMP_FOLDER.newFolder()), cacheProvider);

    // Write out a location
    Location location = lf.create("test");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Check if the cache file is there
    long oldLastModified = location.lastModified();
    Path cachedFile = cacheProvider.getCachePath(cacheProvider.getCacheName(location), oldLastModified);
    Assert.assertTrue(Files.exists(cachedFile));
    try (InputStream is = Files.newInputStream(cachedFile)) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Sleep a second to have the last modified time change
    TimeUnit.SECONDS.sleep(1);

    // Update the location with new content
    message = "Testing message 2";
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Check if the cache file is there
    cachedFile = cacheProvider.getCachePath(cacheProvider.getCacheName(location), location.lastModified());
    Assert.assertTrue(Files.exists(cachedFile));
    try (InputStream is = Files.newInputStream(cachedFile)) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Clear the old entry
    cacheProvider.clearCache(location.getName(), oldLastModified);

    // The new cache entry should still be there
    cachedFile = cacheProvider.getCachePath(cacheProvider.getCacheName(location), location.lastModified());
    Assert.assertTrue(Files.exists(cachedFile));

    // Clear all cache entries. The caching directory for the file should be gone.
    cacheProvider.clearCache();
    Path cacheFileDir = cacheProvider.getCachePath(cacheProvider.getCacheName(location),
                                                   location.lastModified()).getParent();
    Assert.assertFalse(Files.exists(cacheFileDir));
  }

  @Test
  public void testCachePopulate() throws IOException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    DefaultCachingPathProvider cacheProvider = new DefaultCachingPathProvider(cachePath, 1, TimeUnit.HOURS);
    LocationFactory lf = new CachingLocationFactory(new LocalLocationFactory(TEMP_FOLDER.newFolder()), cacheProvider);

    // Write out a location
    Location location = lf.create("test");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    // Read it back so that the cache is populated
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Create a new preview cache provider from the same directory, it should
    // populate the cache.
    Collection<Path> cacheEntries = new DefaultCachingPathProvider(cachePath, 1, TimeUnit.HOURS).getCacheEntries();
    Assert.assertTrue(cacheEntries.contains(cacheProvider.getCachePath(cacheProvider.getCacheName(location),
        location.lastModified())));
  }

  @Test
  public void testNoDuplicateCacheEntries() throws IOException, InterruptedException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    // Create a new cache provider from the same directory, it should
    // populate the cache.
    DefaultCachingPathProvider cachingPathProvider = new DefaultCachingPathProvider(cachePath, 1, TimeUnit.HOURS);
    LocationFactory lf = new CachingLocationFactory(
      new LocalLocationFactory(TEMP_FOLDER.newFolder()), cachingPathProvider);

    // Write out a location
    Location location = lf.create("test1");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }

    // Add the location to cache, a new entry should be added to the cache map.
    cachingPathProvider.apply(location);
    Assert.assertEquals(1, cachingPathProvider.getCacheEntries().size());

    // Re-add the location to cache, cache size should remain the same.
    cachingPathProvider.apply(location);
    Assert.assertEquals(1, cachingPathProvider.getCacheEntries().size());

    // Modify the file, last modified should change, adding a new entry in the cache map.
    // Sleep for a while to ensure at least 1 second has passed.
    // Because 1 second is the minimum resolution of the last modified time in MacOS.
    Thread.sleep(1000);
    try (OutputStream os = location.getOutputStream()) {
      os.write((message + message).getBytes(StandardCharsets.UTF_8));
    }

    cachingPathProvider.apply(location);
    Assert.assertEquals(2, cachingPathProvider.getCacheEntries().size());

    // Add another location to the cache map, a new entry should get added
    // to the cache map.
    Location anotherLocation = lf.create("test2");
    try (OutputStream os = anotherLocation.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }

    cachingPathProvider.apply(anotherLocation);
    Assert.assertEquals(3, cachingPathProvider.getCacheEntries().size());
  }

  @Test (expected = IOException.class)
  public void testDelete() throws IOException {
    Path cachePath = TEMP_FOLDER.newFolder().toPath();
    String message = "Testing message";

    DefaultCachingPathProvider cacheProvider = new DefaultCachingPathProvider(cachePath, 1, TimeUnit.HOURS);
    LocationFactory lf = new CachingLocationFactory(new LocalLocationFactory(TEMP_FOLDER.newFolder()), cacheProvider);

    // Write out a location
    Location location = lf.create("test");
    try (OutputStream os = location.getOutputStream()) {
      os.write(message.getBytes(StandardCharsets.UTF_8));
    }
    // Read it back so that the cache is populated
    try (InputStream is = location.getInputStream()) {
      Assert.assertEquals(message, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Delete the location and try to read. Exception should be thrown instead of
    // reading from the cache.
    location.delete();
    location.getInputStream();
  }
}
