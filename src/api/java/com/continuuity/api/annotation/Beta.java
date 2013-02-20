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
 * This annotation specifies a public API is probably subjected to incompatible
 * changes to extent of removal from libray in future releases. This annotation
 * if declared on API is exempting it from compatibility gurantees made by it's
 * library.
 *
 * <p>
 *   Note that just having this annotation does not imply difference in quality
 *   with API's are non-beta or also they are "NOT" inferior in terms of performance
 *   with their non-beta counter parts. This annotation just signifies that the
 *   API's have not been hardened yet and could be subjected to change in near future.
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
