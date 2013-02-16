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
  public static final Attributes.Name PROCESSOR_TYPE = new Attributes.Name("Processor-Type");
  public static final Attributes.Name PROCESS_NAME = new Attributes.Name("Processor-Name");
  public static final Attributes.Name SPEC_FILE = new Attributes.Name("Spec-File");
}
