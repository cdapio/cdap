/*
 * Copyright © 2024 Cask Data, Inc.
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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultCachingPathProvider}.
 */
public class DefaultCachingPathProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGetCacheNameExactMd5AndLifecycle() throws Exception {
    Path cacheDir = TEMP_FOLDER.newFolder().toPath();
    final DefaultCachingPathProvider provider = new DefaultCachingPathProvider(
        cacheDir, 1, TimeUnit.HOURS, 0);

    Location location = Mockito.mock(Location.class);
    Mockito.when(location.toURI()).thenReturn(URI.create("file:///dir/artifact.jar"));
    Mockito.when(location.getName()).thenReturn("artifact.jar");
    Mockito.when(location.lastModified()).thenReturn(1700000000000L);

    // Baseline MD5 of "/dir/artifact.jar" via Guava Hashing.md5().hashString(CharSequence) (UTF-16LE).
    String expectedCacheName = "86ad2f248fc59b75d2deefb36eebb50d-artifact.jar";
    Assert.assertEquals(expectedCacheName, provider.getCacheName(location));

    Optional<Path> cachePath = provider.apply(location);
    Assert.assertTrue(cachePath.isPresent());
    Path expectedPath = cacheDir.resolve(expectedCacheName)
        .resolve("1700000000000-" + expectedCacheName);
    Assert.assertEquals(expectedPath, cachePath.get());

    // Materialize the cached file and verify a newly constructed provider populates it from disk.
    Files.createDirectories(expectedPath.getParent());
    Files.write(expectedPath, "cached".getBytes(StandardCharsets.UTF_8));

    DefaultCachingPathProvider reloaded = new DefaultCachingPathProvider(
        cacheDir, 1, TimeUnit.HOURS, 0);
    Assert.assertTrue(reloaded.getCacheEntries().contains(expectedPath));

    // Clearing the specific entry deletes the cached file and empty parent directory.
    provider.clearCache(expectedCacheName, 1700000000000L);
    Assert.assertFalse(Files.exists(expectedPath));

    // When location.lastModified() throws IOException, apply returns Optional.empty().
    Location failingLocation = Mockito.mock(Location.class);
    Mockito.when(failingLocation.toURI()).thenReturn(URI.create("file:///dir/bad.jar"));
    Mockito.when(failingLocation.getName()).thenReturn("bad.jar");
    Mockito.when(failingLocation.lastModified()).thenThrow(new IOException("stat failed"));
    Assert.assertFalse(provider.apply(failingLocation).isPresent());
  }
}
