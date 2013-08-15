/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for Guice bindings for classes that are specific for TxDs2 integration.
 *
 * NOTE: This annotation is for temporary use during the transition to the new TxDs2 system.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface TxDs2 {

}
