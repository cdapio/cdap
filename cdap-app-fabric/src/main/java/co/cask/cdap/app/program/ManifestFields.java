/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.program;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Defines the attributes as constants that are being used in
 * manifest file.
 */
public final class ManifestFields {

  public static final Attributes.Name MAIN_CLASS = Attributes.Name.MAIN_CLASS;
  public static final Attributes.Name MANIFEST_VERSION = Attributes.Name.MANIFEST_VERSION;
  public static final Attributes.Name PROGRAM_TYPE = new Attributes.Name("Processor-Type");
  public static final Attributes.Name SPEC_FILE = new Attributes.Name("Spec-File");

  public static final Attributes.Name ACCOUNT_ID = new Attributes.Name("Account-Id");
  public static final Attributes.Name APPLICATION_ID = new Attributes.Name("Application-Id");
  public static final Attributes.Name PROGRAM_NAME = new Attributes.Name("Program-Name");

  // This following are attributes for OSGI bundle
  public static final Attributes.Name BUNDLE_VERSION = new Attributes.Name("Bundle-Version");
  public static final Attributes.Name EXPORT_PACKAGE = new Attributes.Name("Export-Package");

  public static final String VERSION = "1.0";   // Defines manifest version value.
  public static final String MANIFEST_SPEC_FILE = "META-INF/specification/application.json";

  /**
   * Regex for extracting one package name from the Export-Package manifest entry.
   * Refer to OSGI spec r4v43, section 3.6.5 for details. For example:
   *
   * co.cask.plugin;use:="\"test,test2\"";version="1.0",co.cask.plugin2
   *
   * It basically is a comma separated list of package information.
   * Each package information is started with the package name, followed by number of directives or attributes.
   *
   * * A "directive" is a key value pair separate by ":="
   * A "attribute" is a key value pair separated by "=".
   *
   * The following regex is for just extracting the package name (e.g. "co.cask.plugin" and "co.cask.plugin2") while
   * matching the complete package information pattern.
   */
  private static final Pattern EXPORT_PACKAGE_PATTERN =
    Pattern.compile("([\\w.]+)(?:;[\\w\\-.]+:?=(?:(?:\"(?:\\\\.|[^\\\"])+\")|[\\w\\-.]+))*,?");

  /**
   * Parses the manifest {@link #EXPORT_PACKAGE} attribute and returns a set of export packages.
   */
  public static Set<String> getExportPackages(@Nullable Manifest manifest) {
    if (manifest == null) {
      return ImmutableSet.of();
    }

    ImmutableSet.Builder<String> result = ImmutableSet.builder();
    String exportPackages = manifest.getMainAttributes().getValue(ManifestFields.EXPORT_PACKAGE);
    if (exportPackages == null) {
      return result.build();
    }

    // The matcher matches the package name one by one.
    Matcher matcher = EXPORT_PACKAGE_PATTERN.matcher(exportPackages);
    int start = 0;
    while (matcher.find(start)) {
      result.add(matcher.group(1));
      start = matcher.end();
    }
    return result.build();
  }

  private ManifestFields() {
  }
}
