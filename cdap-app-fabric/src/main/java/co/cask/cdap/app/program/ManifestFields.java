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

import java.util.jar.Attributes;

/**
 * Defines the attributes as constants that are being used in
 * manifest file.
 */
public final class ManifestFields {
  public static final Attributes.Name MAIN_CLASS = Attributes.Name.MAIN_CLASS;
  public static final Attributes.Name MANIFEST_VERSION = Attributes.Name.MANIFEST_VERSION;
  public static final Attributes.Name PROGRAM_TYPE = new Attributes.Name("Program-Type");
  public static final Attributes.Name SPEC_FILE = new Attributes.Name("Spec-File");

  public static final Attributes.Name ACCOUNT_ID = new Attributes.Name("Account-Id");
  public static final Attributes.Name APPLICATION_ID = new Attributes.Name("Application-Id");
  public static final Attributes.Name PROGRAM_NAME = new Attributes.Name("Program-Name");

  public static final String VERSION = "1.0";   // Defines manifest version value.
  public static final String MANIFEST_SPEC_FILE = "META-INF/specification/application.json";
}
