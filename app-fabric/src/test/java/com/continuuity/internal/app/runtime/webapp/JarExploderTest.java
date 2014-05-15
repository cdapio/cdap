package com.continuuity.internal.app.runtime.webapp;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.jar.JarEntry;

/**
 * Test jar exploder.
 */
public class JarExploderTest {

  private Map<String, String> aFileContentMap = ImmutableMap.of(
    "test_explode/a/a1.txt", "a100",
    "test_explode/a/a2.txt", "a200",
    "test_explode/a/x/x1.txt", "x100"
  );

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testExplodeA() throws Exception {
    URL jarUrl = getClass().getResource("/test_explode.jar");
    Assert.assertNotNull(jarUrl);

    File dest = TEMP_FOLDER.newFolder();

    int numFiles = JarExploder.explode(new File(jarUrl.toURI()), dest, new Predicate<JarEntry>() {
      @Override
      public boolean apply(JarEntry input) {
        return input.getName().startsWith("test_explode/a");
      }
    });

    Assert.assertEquals(aFileContentMap.size(), numFiles);

    verifyA(dest);
  }

  private void verifyA(File dest) throws Exception {
    Collection<File> files = FileUtils.listFiles(dest, new String[]{"txt"}, true);
    Assert.assertEquals(aFileContentMap.size(), files.size());

    for (File f : files) {
      String name = f.getAbsolutePath().replaceFirst(dest.getAbsolutePath(), "");
      name = name.startsWith("/") ? name.substring(1, name.length()) : name;

      String expected = aFileContentMap.get(name);
      Assert.assertNotNull(expected);

      String actual = Files.toString(f, Charsets.US_ASCII);
      Assert.assertEquals(expected, actual.trim());
    }
  }
}
