/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import java.util.jar.Attributes;

/**
 * Defines the attributes as constants that are being used in
 * manifest file.
 */
public final class ManifestFields {
  public static final Attributes.Name MAIN_CLASS = Attributes.Name.MAIN_CLASS;
  public static final Attributes.Name MANIFEST_VERSION = Attributes.Name.MANIFEST_VERSION;
  public static final Attributes.Name PROCESSOR_TYPE = new Attributes.Name("Processor-Type");
  public static final Attributes.Name SPEC_FILE = new Attributes.Name("Spec-File");

  public static final Attributes.Name ACCOUNT_ID = new Attributes.Name("Account-Id");
  public static final Attributes.Name APPLICATION_ID = new Attributes.Name("Application-Id");
  public static final Attributes.Name PROGRAM_NAME = new Attributes.Name("Program-Name");

  public static final String VERSION = "1.0";   // Defines manifest version value.
  public static final String MANIFEST_SPEC_FILE = "META-INF/specification/application.json";
}
