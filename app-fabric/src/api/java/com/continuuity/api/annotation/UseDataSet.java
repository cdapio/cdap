/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a method for {@link com.continuuity.api.data.DataSet} usage.
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 * @see com.continuuity.api.procedure.Procedure Procedure
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface UseDataSet {
  /**
   * Returns name of the {@link com.continuuity.api.data.DataSet}.
   */
  String value();
}
