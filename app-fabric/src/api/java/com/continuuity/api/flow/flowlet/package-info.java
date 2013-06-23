/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * <p>
 *   This package provides API for flowlet implementations.
 * </p>
 * <p>
 *   Every flowlet should implement {@link Flowlet} interface. The packages comes with convenient
 *   {@link AbstractFlowlet} class which has default implementation of {@link Flowlet} methods for easy extension.
 * </p>
 * <p>
 *   A special kind of flowlet that has no inputs but can generate events internally - generator flowlet - is defined by
 *   {@link GeneratorFlowlet} interface. It is encouraged to use {@link AbstractGeneratorFlowlet} class which in future
 *   will implement default behaviour for {@link GeneratorFlowlet} methods.
 * </p>
 */
package com.continuuity.api.flow.flowlet;