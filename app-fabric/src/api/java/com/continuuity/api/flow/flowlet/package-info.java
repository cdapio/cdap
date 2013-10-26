/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * <p>
 *   Flowlet implementations.
 * </p>
 * <p>
 *   Every flowlet must implement the {@link com.continuuity.api.flow.flowlet.Flowlet} interface. 
 *   This package has a convenient {@link com.continuuity.api.flow.flowlet.AbstractFlowlet} class that has a 
 *   default implementation of {@link com.continuuity.api.flow.flowlet.Flowlet} methods for easy extension.
 * </p>
 * <p>
 *   Use the {@link com.continuuity.api.annotation.Tick} annotation to 
 *   tag a {@link com.continuuity.api.flow.flowlet.Flowlet} tick method. Tick methods are called periodically 
 *   by the runtime system.
 * </p>
 */
package com.continuuity.api.flow.flowlet;
