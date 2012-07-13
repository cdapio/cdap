package com.continuuity.common.service.distributed.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

/**
 * Handles copying files from the client machine out to HDFS for app master and container tasks, and
 * then mapping them properly to the LocalResource objects used by YARN.
 */
public class DFSUtility {

  private static Log LOG = LogFactory.getLog(DFSUtility.class);

  // Provide a way for tests/clients to override the app base directory.
  public static final String APP_BASE_DIR = "kitten.app.base.dir";

  public static InputStream getFileOrResource(String name) {
    File f = new File(name);
    if (f.exists()) {
      try {
        return new FileInputStream(f);
      } catch (FileNotFoundException e) {
        LOG.error("A file suddenly disappeared", e);
      }
    } else {
      return DFSUtility.class.getResourceAsStream(name);
    }
    return null;
  }

  private final ApplicationId applicationId;
  private final Configuration conf;
  private final Map<String, URI> localToHdfs;
  private final Set<String> names;

  public DFSUtility(ApplicationId applicationId, Configuration conf) {
    this.applicationId = applicationId;
    this.conf = conf;
    this.localToHdfs = Maps.newHashMap();
    this.names = Sets.newHashSet();
  }

  public void copyConfiguration(String key, Configuration conf) throws IOException {
    File tmpFile = File.createTempFile("job", ".xml");
    tmpFile.deleteOnExit();
    OutputStream os = new FileOutputStream(tmpFile);
    conf.writeXml(os);
    os.close();
    copyToHdfs(key, tmpFile.getAbsolutePath());
  }

  public void copyToHdfs(String localDataName) throws IOException {
    copyToHdfs(localDataName, localDataName);
  }

  private void copyToHdfs(String key, String localDataName) throws IOException {
    if (!localToHdfs.containsKey(localDataName)) {
      FileSystem fs = FileSystem.get(conf);
      Path src = new Path(localDataName);
      Path dst = getPath(fs, src.getName());
      InputStream data = getFileOrResource(localDataName);
      FSDataOutputStream os = fs.create(dst, true);
      ByteStreams.copy(data, os);
      os.close();
      URI uri = dst.toUri();
      localToHdfs.put(key, uri);
    }
  }

  public URI getResourceURI(String key) {
    return localToHdfs.get(key);
  }

  private Path getPath(FileSystem fs, String name) {
    int cp = 0;
    while (names.contains(name)) {
      name = name + (++cp);
    }
    names.add(name);
    String appDir = "flowrunner";
    if (applicationId != null) {
      appDir += applicationId.getId();
    }
    Path base = getAppPath(fs, appDir);
    Path dst = new Path(base, name);
    return dst;
  }

  private Path getAppPath(FileSystem fs, String appDir) {
    String abd = conf.get(APP_BASE_DIR);
    if (abd != null) {
      return new Path(new Path(abd), appDir);
    } else {
      return new Path(fs.getHomeDirectory(), appDir);
    }
  }

  public Map<String, URI> getFileMapping() {
    return localToHdfs;
  }
}
