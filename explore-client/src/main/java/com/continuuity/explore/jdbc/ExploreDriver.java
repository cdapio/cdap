package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExternalAsyncExploreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver.
 */
public class ExploreDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDriver.class);
  static {
    try {
      java.sql.DriverManager.registerDriver(new ExploreDriver());
    } catch (SQLException e) {
      LOG.error("Caught exception when registering Reactor JDBC Driver", e);
    }
  }

  private static final Pattern CONNECTION_URL_PATTERN = Pattern.compile(ExploreJDBCUtils.URL_PREFIX + ".*");

  // The explore jdbc driver is not JDBC compliant, as tons of functionalities are missing
  private static final boolean JDBC_COMPLIANT = false;

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();
    ExploreClient exploreClient = new ExternalAsyncExploreClient(host, port);
    if (!exploreClient.isAvailable()) {
      throw new SQLException("Cannot connect to {}, service unavailable", url);
    }
    return new ExploreConnection(exploreClient);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return CONNECTION_URL_PATTERN.matcher(url).matches();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    throw new SQLException("Method not supported" + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public int getMajorVersion() {
    // TODO implement [REACTOR-319]
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO implement [REACTOR-319]
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  /**
   * Package scoped access to the Driver's Major Version
   * @return The Major version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  public static int getMajorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = fetchManifestAttribute(
        Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\."); //$NON-NLS-1$

      if (tokens != null && tokens.length > 0 && tokens[0] != null) {
        version = Integer.parseInt(tokens[0]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
      version = -1;
    }
    return version;
  }

  /**
   * Package scoped access to the Driver's Minor Version
   * @return The Minor version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  public static int getMinorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = ExploreDriver.fetchManifestAttribute(
        Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\."); //$NON-NLS-1$

      if (tokens != null && tokens.length > 1 && tokens[1] != null) {
        version = Integer.parseInt(tokens[1]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
      version = -1;
    }
    return version;
  }

  /**
   * Lazy-load manifest attributes as needed.
   * TODO: Add manifest attributes
   */
  private static Attributes manifestAttributes = null;

  /**
   * Loads the manifest attributes from the jar.
   *
   * @throws java.net.MalformedURLException
   * @throws java.io.IOException
   */
  private static synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }
    Class<?> clazz = ExploreDriver.class;
    String classContainer = clazz.getProtectionDomain().getCodeSource()
      .getLocation().toString();
    URL manifestUrl = new URL("jar:" + classContainer
                                + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();
  }

  /**
   * Package scoped to allow manifest fetching from other HiveDriver classes
   * Helper to initialize attributes and return one.
   *
   * @param attributeName
   * @return
   * @throws SQLException
   */
  public static String fetchManifestAttribute(Attributes.Name attributeName)
    throws SQLException {
    try {
      loadManifestAttributes();
    } catch (IOException e) {
      throw new SQLException("Couldn't load manifest attributes.", e);
    }
    return manifestAttributes.getValue(attributeName);
  }

}
