package com.continuuity.common.classloader;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 *  Testing of JarClassLoader.
 */
public class JarClassLoaderTest {

  private File root = null;

  @Before
  public void setUp() throws Exception {

    // Creates a sample class, compiles it and creates a jar file.
    String source = "public class Me { public Me() {} public String getName() {" +
      " return \"continuuity\"; } }";

    root = new File(new File("").getAbsolutePath() + "/build/tmp");
    File sourceFile = new File(root, "me/Me.java");
    sourceFile.getParentFile().mkdirs();
    new FileWriter(sourceFile).append(source).close();

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    compiler.run(null, null, null, sourceFile.getPath());

    File classFile = new File(root, "me/Me.class");
    File[] sources = { classFile };
    add(new File(root, "test.jar"), sources);
  }

  private void add(File archiveFile, File[] sources) throws IOException {
    byte buffer[] = new byte[10240];

    FileOutputStream stream = new FileOutputStream(archiveFile);
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream out = new JarOutputStream(stream, manifest);

    for(int i = 0; i < sources.length; ++i) {
      if(sources[i] == null || ! sources[i].exists() || sources[i].isDirectory()) continue;

      JarEntry entry = new JarEntry(sources[i].getName());
      entry.setTime(sources[i].lastModified());
      out.putNextEntry(entry);
      FileInputStream in = new FileInputStream(sources[i]);
      while(true) {
        int nRead = in.read(buffer, 0, buffer.length);
        if(nRead <= 0) {
          break;
        }
        out.write(buffer, 0, nRead);
      }
      in.close();
    }
    out.close();
    stream.close();
  }

  @Test
  public void testJarClassLoader() throws Exception {
    String path = root.getAbsolutePath();
    String jarFile = path + "/test.jar";
    JarClassLoader classLoader = new JarClassLoader(jarFile);
    Assert.assertNotNull(classLoader);
    Class<?> clazz = classLoader.loadClass("Me");
    Assert.assertNotNull(clazz);
    Object o = clazz.newInstance();
    Assert.assertNotNull(o);
  }
}
