package com.continuuity.api.flow.flowlet;

/**
 * Default implementation of {@link Flowlet} for easy extension.
 * It uses result of {@link #getName()} as the Flowlet name
 * and result of {@link #getDescription()}} as the Flowlet description.
 * <p>
 *   Children classes can overrides the {@link #getName()}} and/or {@link #getDescription()}
 *   methods to have custom namings. Children can also overrides the {@link #configure()} method
 *   to have more controls on customization the {@link FlowletSpecification}.
 * </p>
 */
public abstract class AbstractGeneratorFlowlet extends AbstractFlowlet implements GeneratorFlowlet {

}
