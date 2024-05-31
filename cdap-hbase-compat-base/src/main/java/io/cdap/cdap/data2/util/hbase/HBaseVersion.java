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

package io.cdap.cdap.data2.util.hbase;

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.output.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects the currently loaded HBase version.  It is assumed that only one HBase version is loaded
 * at a time, since using more than one HBase version within the same process will require
 * classloader isolation anyway.
 */
public class HBaseVersion {

  private static final String HBASE_94_VERSION = "0.94";
  private static final String HBASE_96_VERSION = "0.96";
  private static final String HBASE_98_VERSION = "0.98";
  private static final String HBASE_10_VERSION = "1.0";
  private static final String HBASE_11_VERSION = "1.1";
  private static final String HBASE_12_VERSION = "1.2";
  private static final String HBASE_13_VERSION = "1.3";
  private static final String CDH55_CLASSIFIER = "cdh5.5.";
  private static final String CDH56_CLASSIFIER = "cdh5.6.";
  private static final String CDH57_CLASSIFIER = "cdh5.7.";
  private static final String CDH58_CLASSIFIER = "cdh5.8.";
  private static final String CDH59_CLASSIFIER = "cdh5.9.";
  private static final String CDH510_CLASSIFIER = "cdh5.10.";
  private static final String CDH511_CLASSIFIER = "cdh5.11.";
  private static final String CDH512_CLASSIFIER = "cdh5.12.";
  private static final String CDH513_CLASSIFIER = "cdh5.13.";
  private static final String CDH514_CLASSIFIER = "cdh5.14.";
  private static final String CDH515_CLASSIFIER = "cdh5.15.";
  private static final String CDH_CLASSIFIER = "cdh";

  private static final Logger LOG = LoggerFactory.getLogger(HBaseVersion.class);

  /**
   * Represents the major version of the HBase library that is currently loaded.
   */
  public enum Version {
    HBASE_94("0.94"),
    HBASE_96("0.96"),
    HBASE_98("0.98"),
    HBASE_10("1.0"),
    HBASE_10_CDH("1.0-cdh"),
    HBASE_10_CDH55("1.0-cdh5.5"),
    HBASE_10_CDH56("1.0-cdh5.6"),
    HBASE_11("1.1"),
    HBASE_12_CDH57("1.2-cdh5.7"),
    UNKNOWN("unknown"),
    UNKNOWN_CDH("unknown-cdh");

    final String majorVersion;

    Version(String majorVersion) {
      this.majorVersion = majorVersion;
    }

    public String getMajorVersion() {
      return majorVersion;
    }
  }

  private static Version currentVersion;
  private static String versionString;

  /**
   * Returns the major version of the currently loaded HBase library.
   */
  public static synchronized Version get() {
    if (currentVersion != null) {
      return currentVersion;
    }
    determineVersion();
    return currentVersion;
  }

  /**
   * Returns the full version string for the currently loaded HBase library.
   */
  public static synchronized String getVersionString() {
    if (versionString != null) {
      return versionString;
    }
    determineVersion();
    return versionString;
  }

  /**
   * Prints out the HBase {@link Version} enum value for the current version of HBase on the
   * classpath.
   */
  public static void main(String[] args) {
    // Suppress any output to stdout
    PrintStream stdout = System.out;
    System.setOut(new PrintStream(new NullOutputStream()));
    Version version = HBaseVersion.get();

    // Restore stdout
    System.setOut(stdout);
    System.out.println(version.getMajorVersion());

    if (args.length == 1 && "-v".equals(args[0])) {
      // Print versionString if verbose
      System.out.println("versionString=" + getVersionString());
    }
  }

  /**
   * Utility class to parse apart version number components.  The version string provided is
   * expected to be in one of the formats below.
   *
   * <p>Only the major version number and classifier are actually required.</p>
   */
  public static class VersionNumber {

    // Common pattern (for CDH, open source distro, etc.)
    // major[.minor[.patch[.last]][-classifier][-SNAPSHOT]
    private static final Pattern PATTERN =
        Pattern.compile(
            "(\\d+)(\\.(\\d+))?(\\.(\\d+))?(\\.(\\d+))?(-(?!SNAPSHOT)([^\\-]+))?(-SNAPSHOT)?");

    // HDP has a different format:
    // major.minor.patch.hdp_major.hdp_minor.hdp_patch.hdp_last-package_number[-hadoop2],
    // For now, we support only non-snapshot versions
    private static final Pattern HDP_PATTERN =
        Pattern.compile(
            "(\\d+)(\\.(\\d+))(\\.(\\d+))(\\.(\\d+))(\\.(\\d+))(\\.(\\d+))(\\.(\\d+))(-(\\d+))(-hadoop2)?");

    // IBM has a different format where they add build number at the end,
    // major[.minor[.patch[.last]][-classifier][-buildNumber],
    // we ignore build number for now and support only non-snapshot versions.
    private static final Pattern IBM_PATTERN =
        Pattern.compile(
            "(\\d+)(\\.(\\d+))?(\\.(\\d+))?(\\.(\\d+))?(-(?!SNAPSHOT)([^\\-]+))?(-(\\d+))?");

    private Integer major;
    private Integer minor;
    private Integer patch;
    private Integer last;
    private String classifier;
    private boolean snapshot;

    private VersionNumber(Integer major, Integer minor, Integer patch, Integer last,
        String classifier, boolean snapshot) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.last = last;
      this.classifier = classifier;
      this.snapshot = snapshot;
    }

    public Integer getMajor() {
      return major;
    }

    public Integer getMinor() {
      return minor;
    }

    public Integer getPatch() {
      return patch;
    }

    public Integer getLast() {
      return last;
    }

    public String getClassifier() {
      return classifier;
    }

    public boolean isSnapshot() {
      return snapshot;
    }

    public static VersionNumber create(String versionString) throws ParseException {
      Matcher matcher = PATTERN.matcher(versionString);
      Matcher hdpMatcher = HDP_PATTERN.matcher(versionString);
      Matcher ibmMatcher = IBM_PATTERN.matcher(versionString);
      if (matcher.matches()) {
        String majorString = matcher.group(1);
        String minorString = matcher.group(3);
        String patchString = matcher.group(5);
        String last = matcher.group(7);
        String classifier = matcher.group(9);
        String snapshotString = matcher.group(10);
        return new VersionNumber(new Integer(majorString),
            minorString != null ? new Integer(minorString) : null,
            patchString != null ? new Integer(patchString) : null,
            last != null ? new Integer(last) : null,
            classifier,
            "-SNAPSHOT".equals(snapshotString));
      } else if (hdpMatcher.matches()) {
        String majorString = hdpMatcher.group(1);
        String minorString = hdpMatcher.group(3);
        String patchString = hdpMatcher.group(5);
        return new VersionNumber(new Integer(majorString),
            minorString != null ? new Integer(minorString) : null,
            patchString != null ? new Integer(patchString) : null,
            null, null, false);
      } else if (ibmMatcher.matches()) {
        String majorString = ibmMatcher.group(1);
        String minorString = ibmMatcher.group(3);
        String patchString = ibmMatcher.group(5);
        String last = ibmMatcher.group(7);
        String classifier = ibmMatcher.group(9);
        return new VersionNumber(new Integer(majorString),
            minorString != null ? new Integer(minorString) : null,
            patchString != null ? new Integer(patchString) : null,
            last != null ? new Integer(last) : null,
            classifier,
            false);
      }
      throw new ParseException(
          "Input string did not match expected pattern: major[.minor[.patch]][-classifier][-SNAPSHOT]",
          0);
    }
  }

  private static void determineVersion() {
    try {
      Class<?> versionInfoClass = Class.forName("org.apache.hadoop.hbase.util.VersionInfo");
      Method versionMethod = versionInfoClass.getMethod("getVersion");
      versionString = (String) versionMethod.invoke(null);
      currentVersion = determineVersionFromVersionString(versionString);
    } catch (ClassNotFoundException e) {
      // If there is no HBase class, treat it as unknown. This is for env that HBase is not available.
      LOG.debug("HBase is not available from the environment. Cannot determine HBase version.");
    } catch (Throwable e) {
      LOG.error("Unable to determine HBase version", e);
      currentVersion = Version.UNKNOWN;
    }

    if (versionString == null) {
      versionString = "unknown";
    }
  }

  @VisibleForTesting
  static Version determineVersionFromVersionString(String versionString) throws ParseException {
    if (versionString.startsWith(HBASE_94_VERSION)) {
      return Version.HBASE_94;
    }
    if (versionString.startsWith(HBASE_96_VERSION)) {
      return Version.HBASE_96;
    }
    if (versionString.startsWith(HBASE_98_VERSION)) {
      return Version.HBASE_98;
    }
    VersionNumber ver = VersionNumber.create(versionString);
    if (versionString.startsWith(HBASE_10_VERSION)) {
      return getHBase10VersionFromVersion(ver);
    }
    if (versionString.startsWith(HBASE_11_VERSION)) {
      return Version.HBASE_11;
    }
    if (versionString.startsWith(HBASE_12_VERSION)) {
      return getHBase12VersionFromVersion(ver);
    }
    boolean isCDH = ver.getClassifier() != null && ver.getClassifier().startsWith(CDH_CLASSIFIER);
    // hbase 1.3 packaged with any future CDH version will likely not be compatible, since the hbase 1.2 version with
    // CDH is not compatible, whereas the HDP and OSS versions are
    if (versionString.startsWith(HBASE_13_VERSION) && !isCDH) {
      return Version.HBASE_11;
    }
    if (isCDH) {
      return Version.UNKNOWN_CDH;
    }
    return Version.UNKNOWN;
  }

  private static Version getHBase10VersionFromVersion(VersionNumber ver) {
    if (ver.getClassifier() != null && ver.getClassifier().startsWith(CDH_CLASSIFIER)) {
      if (ver.getClassifier().startsWith(CDH55_CLASSIFIER)) {
        return Version.HBASE_10_CDH55;
      }
      if (ver.getClassifier().startsWith(CDH56_CLASSIFIER)) {
        return Version.HBASE_10_CDH56;
      }
      return Version.HBASE_10_CDH;
    }
    return Version.HBASE_10;
  }

  private static Version getHBase12VersionFromVersion(VersionNumber ver) {
    if (ver.getClassifier() != null && ver.getClassifier().startsWith(CDH_CLASSIFIER)) {
      if (ver.getClassifier().startsWith(CDH57_CLASSIFIER)
          || // CDH 5.7 compat module can be re-used with CDH 5.[8-14].x
          ver.getClassifier().startsWith(CDH58_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH59_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH510_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH511_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH512_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH513_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH514_CLASSIFIER)
          || ver.getClassifier().startsWith(CDH515_CLASSIFIER)) {
        return Version.HBASE_12_CDH57;
      }
      return Version.UNKNOWN_CDH;
    } else {
      // HBase-11 compat module can be re-used for HBASE-12 as there is no change needed in compat source.
      return Version.HBASE_11;
    }
  }
}
