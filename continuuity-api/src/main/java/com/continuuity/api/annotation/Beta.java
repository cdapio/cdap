/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation specifies a public API that is probably subjected to incompatible
 * changes up to the extent of removal from the library in future releases. This annotation
 * if declared on an API is exempting it from compatibility guarantees made by its
 * library.
 *
 * <p>
 *   Note that just having this annotation does not imply differences in quality
 *   with API's that are non-beta or also they are "NOT" inferior in terms of performance
 *   with their non-beta counter parts. This annotation just signifies that the
 *   API's have not been hardened yet and could be subject to change in the near future.
 * </p>
 */
@Retention(RetentionPolicy.CLASS)
@Target({
          ElementType.ANNOTATION_TYPE,
          ElementType.CONSTRUCTOR,
          ElementType.FIELD,
          ElementType.METHOD,
          ElementType.TYPE})
@Documented
public @interface Beta {
}
