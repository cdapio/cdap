/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.verification;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.app.verification.AbstractVerifier;

/**
 * @param <T> Type of ProgramSpecification
 */
public class ProgramVerification<T extends ProgramSpecification> extends AbstractVerifier<T> {

  @Override
  protected String getName(T input) {
    return input.getName();
  }
}
