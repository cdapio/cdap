/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * <p>
 *   Flowlet implementations.
 * </p>
 * <p>
 *   Every flowlet must implement the {@link Flowlet} interface. This package has a convenient
 *   {@link AbstractFlowlet} class that has a default implementation of {@link Flowlet} methods for easy extension.
 * </p>
 * <p>
 *   Use the {@link Tick} annotation to tag a {@link Flowlet} tick method. Tick methods are called periodically by 
 * the runtime system.
 * </p>
 */
package com.continuuity.api.flow.flowlet;
