package com.continuuity.manager.archive.internal;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.common.builder.Builder;
import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.classloader.JarClassLoader;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.flow.definition.api.ConnectionDefinition;
import com.continuuity.flow.definition.api.FlowDefinition;
import com.continuuity.flow.definition.api.FlowStreamDefinition;
import com.continuuity.flow.definition.api.FlowletDefinition;
import com.continuuity.flow.definition.api.MetaDefinition;
import com.continuuity.flow.definition.impl.SpecifierFactory;
import com.continuuity.flow.flowlet.internal.StreamsConfigurationAccessor;
import com.continuuity.flow.flowlet.internal.StreamsConfiguratorImpl;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.validator.ValidatorException;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 *
 */
public final class FlowArchiveValidator extends AbstractArchiveValidator<Flow> {

  private static final Logger LOG = LoggerFactory.getLogger(FlowArchiveValidator.class);

  @Override
  protected Status doValidate(File archive, Class<? extends Flow> mainClass, ClassLoader classLoader) throws IllegalArgumentException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected Class<Flow> getClassType() {
    return Flow.class;
  }

//  /**
//   * Validates the schemas of the connecting flowlets.
//   *
//   * @param streams     connecting flowlets.
//   * @param flowStreams definition providing information about how the flow
//   *                    is connected externally
//   * @param connections definition providing information about how the flowlets
//   *                    are connected.
//   * @return pair consisting of status of validation of schema and proper
//   *         message if errors.
//   */
//  private ImmutablePair<Boolean, String> validateSchema(Map<String, StreamsConfigurationAccessor> streams, Collection<? extends FlowStreamDefinition> flowStreams, Collection<? extends ConnectionDefinition> connections) {
//    HashSet<String> flowStreamNames = Sets.newHashSet();
//    for (FlowStreamDefinition stream : flowStreams) {
//      flowStreamNames.add(stream.getName());
//    }
//
//    for (ConnectionDefinition connection : connections) {
//
//      String fromName = connection.getFrom().getFlowlet();
//      String toName = connection.getTo().getFlowlet();
//      String fromStream = connection.getFrom().getStream();
//      String toStream = connection.getTo().getStream();
//      TupleSchema in, out;
//
//      // If you are trying to connect to the same flowlet.
//      if (toName != null && fromName != null && fromName.equals(toName)) {
//        return new ImmutablePair<Boolean, String>(false, String.format("Cannot have a connection to the same flowlet. " + "From '%s', To '%s'", fromName, toName));
//      }
//
//      // determine the in schema of the destination of the connection
//      if (connection.getTo().isFlowStream()) {
//        // output to flow stream is not yet supported, generate correct error
//        if (connection.getFrom().isFlowStream()) {
//          // flow stream -> flow stream
//          return new ImmutablePair<Boolean, String>(false, String.format("Connecting flow stream %s to flow stream %s is " + "not supported.", fromStream, toStream));
//        } else {
//          // flowlet -> flow stream
//          return new ImmutablePair<Boolean, String>(false, String.format("Connecting flowlet %s:%s to flow stream %s is not " + "supported.", fromName, fromStream, toStream));
//        }
//      } else {
//        if (!streams.containsKey(toName)) {
//          return new ImmutablePair<Boolean, String>(false, String.format("Flowlet '%s' has not defined any input streams.", toName));
//        }
//        if ("in".equals(toStream)) {
//          in = streams.get(toName).getDefaultInputStream().getSchema();
//        } else {
//          in = streams.get(toName).getInputStreams().get(toStream).getSchema();
//        }
//      }
//
//      // determine the out schema of the origin of the connection
//      if (connection.getFrom().isFlowStream()) {
//        if (flowStreamNames.contains(fromStream)) {
//          out = TupleSchema.EVENT_SCHEMA;
//        } else {
//          return new ImmutablePair<Boolean, String>(false, String.format("Flow stream '%s' must be declared to be " + "connected to flowlet '%s'.", fromStream, toName));
//        }
//      } else {
//        if (!streams.containsKey(fromName)) {
//          return new ImmutablePair<Boolean, String>(false, String.format("Flowlet '%s' has not defined any output streams.", fromName));
//        }
//        if ("out".equals(fromStream)) {
//          out = streams.get(fromName).getDefaultOutputStream().getSchema();
//        } else {
//          out = streams.get(fromName).getOutputStreams().
//            get(fromStream).getSchema();
//        }
//      }
//
//      /** Compares tuple schema of two streams */
//      if (out == null || in == null) {
//        return new ImmutablePair<Boolean, String>(false, String.format("No stream have been defined for connecting flowlets" + " '%s' and '%s'", fromName, toName));
//      }
//
//      boolean status = out.equals(in);
//
//      if (!status) {
//        if (connection.getFrom().isFlowStream()) {
//          return new ImmutablePair<Boolean, String>(false, String.format("Flow schema %s connecting to flowlet %s:%s do not have the " + "same schema.", fromStream, toName, toStream));
//        } else {
//          return new ImmutablePair<Boolean, String>(false, String.format("Flowlet %s:%s connecting to %s:%s do not have the same schema.", fromName, fromStream, toName, toStream));
//        }
//      }
//    }
//    return new ImmutablePair<Boolean, String>(true, "OK");
//  }
//
//  /**
//   * Validates the meta section of the flow.
//   * <p/>
//   * <p>
//   * Three fields namely flow name, email & namespace are mandatory fields.
//   * </p>
//   *
//   * @param meta definition of a flow.
//   * @return pair consisting of status and message indicating the error.
//   */
//  private ImmutablePair<Boolean, String> validateMeta(MetaDefinition meta) {
//    if (meta == null) {
//      return new ImmutablePair<Boolean, String>(false, "No meta section defined for the flow");
//    }
//
//    String name = meta.getName();
//    if (name == null || name.isEmpty()) {
//      return new ImmutablePair<Boolean, String>(false, "No name for flow specified");
//    }
//
//    String app = meta.getApp();
//    if (app == null || app.isEmpty()) {
//      return new ImmutablePair<Boolean, String>(false, "No application name specified");
//    }
//
//    String email = meta.getEmail();
//    if (email == null || email.isEmpty()) {
//      return new ImmutablePair<Boolean, String>(false, "No email specified for notification on flow status");
//    }
//    return new ImmutablePair<Boolean, String>(true, "OK");
//  }
//
//  /**
//   * Loads the flowlet provided a class loader, else uses default class loader.
//   *
//   * @param definition of the flow.
//   * @param loader     class loader.
//   * @return an instance of streams configuration as specified by the flowlet.
//   */
//  private StreamsConfigurationAccessor getFlowletStreams(FlowletDefinition definition, JarClassLoader loader) throws ValidatorException {
//
//    LOG.trace("Invoking flowlet#configure to generate streams for flowlet " + "'{}'.", definition.getName());
//
//    Flowlet flowlet;
//    Class<? extends Flowlet> flowletClass;
//    if (loader != null) {
//      String className = definition.getClassName();
//      Class actualClass;
//      try {
//        actualClass = loader.loadClass(className);
//      } catch (ClassNotFoundException e) {
//        LOG.debug(String.format("Unable to load class %s for flowlet %s. " + "Reason: %s", className, definition.getName(), e.getMessage()));
//        throw new ValidatorException(String.format("Unable to load class " + "%s for flowlet %s", className, definition.getName()), e);
//      }
//      if (Flowlet.class.isAssignableFrom(actualClass)) {
//        @SuppressWarnings("unchecked") Class<? extends Flowlet> cl = (Class<? extends Flowlet>) actualClass;
//        flowletClass = cl;
//      } else {
//        String message = String.format("Class %s for flowlet %s does not " + "implement interface Flowlet.", className, definition.getName());
//        LOG.debug(message);
//        throw new ValidatorException(message);
//      }
//    } else {
//      flowletClass = definition.getClazz();
//    }
//
//    LOG.trace("Creating instance for a flowlet class '{}'.", flowletClass.getCanonicalName());
//    try {
//      flowlet = flowletClass.newInstance();
//    } catch (IllegalAccessException e) {
//      String message = String.format("Unable to access class %s for " + "flowlet %s. Reason: %s ", definition.getName(), flowletClass.getName(), e.getMessage());
//      LOG.debug(message);
//      throw new ValidatorException(message, e);
//    } catch (InstantiationException e) {
//      String message = String.format("Unable to instantiate class %s for " + "flowlet %s. Reason: %s ", definition.getName(), flowletClass.getName(), e.getMessage());
//      LOG.debug(message);
//      throw new ValidatorException(message, e);
//    }
//    StreamsConfigurator streamsConfigurator = new StreamsConfiguratorImpl();
//    flowlet.configure(streamsConfigurator);
//    return (StreamsConfigurationAccessor) streamsConfigurator;
//  }

//  @Override
//  public ImmutablePair<FlowDefinition, String> validate(Class<? extends Flow> flowClass, JarClassLoader loader, String jarName) {
//    try {
//      String className = flowClass.getName();
//      LOG.trace("Creating new instance of class '{}'", className);
//      Flow flow = flowClass.newInstance();
//      LOG.trace("Constructing flow definition for class '{}'", className);
//
//      // Get the flow definition.
//      FlowSpecifier specifier = SpecifierFactory.newFlowSpecifier();
//      flow.configure(specifier);
//
//      // TODO refactor so that runtime cast is unnecessary
//      @SuppressWarnings("unchecked") Builder<FlowDefinition> flowDefinitionBuilder = (Builder<FlowDefinition>) specifier;
//      FlowDefinition definition = flowDefinitionBuilder.build();
//
//      LOG.trace("Validating flow defined by class '{}'", className);
//      ImmutablePair<Boolean, String> validation = this.validate(definition, loader);
//      return new ImmutablePair<FlowDefinition, String>(definition, validation.getFirst() ? null : validation.getSecond());
//
//    } catch (InstantiationException e) {
//      LOG.debug("Unable to instantiate class read from manifest in jar {}. " + "Reason : {}", jarName, e.getMessage());
//      LOG.debug(StackTraceUtil.toStringStackTrace(e));
//      throw new ValidatorException(String.format("Unable to instantiate " + "class read from manifest in jar %s. Reason : %s", jarName, e.getMessage()));
//    } catch (IllegalAccessException e) {
//      LOG.debug("Unable to access the class read from manifest in jar {}. " + "Reason : {}", jarName, e.getMessage());
//      LOG.debug(StackTraceUtil.toStringStackTrace(e));
//      throw new ValidatorException(String.format("Unable to access the class " + "read from manifest in jar %s. Reason : %s", jarName, e.getMessage()));
//    } catch (BuilderException e) {
//      LOG.debug("Unable to build query from jar {}. Reason : {}", jarName, e.getMessage());
//      LOG.debug(StackTraceUtil.toStringStackTrace(e));
//      throw new ValidatorException(String.format("Unable to build flow " + "from jar %s. Reason : %s", jarName, e.getMessage()));
//    }
//  }
//
//  /**
//   * Validates a given Flow definition
//   *
//   * @param definition of flow.
//   * @return pair consisting of status of validation and message specifying
//   *         the error.
//   */
//  @Override
//  public final ImmutablePair<Boolean, String> validate(FlowDefinition definition, JarClassLoader loader) {
//    Map<String, StreamsConfigurationAccessor> flowletInstances = Maps.newHashMap();
//
//    /** Validates the meta section of the flow */
//    ImmutablePair<Boolean, String> metaStatus = validateMeta(definition.getMeta());
//    boolean success = metaStatus.getFirst();
//    if (!success) {
//      return metaStatus;
//    }
//
//    /** Checks for non-zero flowlet counts in flow definition */
//    if (definition.getFlowlets().size() < 1) {
//      return new ImmutablePair<Boolean, String>(false, "No flowlets defined in the flow.");
//    }
//
//    /** Checks for non-zero connections in flow definition */
//    if (definition.getFlowlets().size() > 1) {
//      if (definition.getConnections().size() < 1) {
//        return new ImmutablePair<Boolean, String>(false, "No connections defined in the flow between flowlets.");
//      }
//    }
//
//    Collection<? extends FlowletDefinition> flowlets = definition.getFlowlets();
//
//    /** Retrieves streams from flowlet by instantiating it. */
//    try {
//      for (FlowletDefinition flowlet : flowlets) {
//        StreamsConfigurationAccessor streams = getFlowletStreams(flowlet, loader);
//        if (streams == null) {
//          return new ImmutablePair<Boolean, String>(false, String.format("Unable to instantiate flowlet %s, class %s", flowlet.getName(), flowlet.getClassName()));
//        }
//        flowletInstances.put(flowlet.getName(), streams);
//      }
//    } catch (Exception e) {
//      LOG.warn("During validation, failed to retrieve streams for flow {}." + " Reason : {}.", definition.getMeta().getName(), e.getMessage());
//      return new ImmutablePair<Boolean, String>(false, String.format("There was problem during verification of flow. " + "Reason: %s", e.getMessage()));
//    } catch (Throwable e) {
//      LOG.warn("During validation, failed to retrieve streams for flow {}. " + "Reason : Class {} has error {}.", new Object[]{definition.
//        getMeta().getName(), e.getClass().getName(), e.getMessage()});
//      return new ImmutablePair<Boolean, String>(false, String.format("There was problem during verification of flow. " + "Reason: Class %s has error %s", e.getMessage(), e.getClass().getName()));
//    }
//
//    Collection<? extends ConnectionDefinition> connections = definition.getConnections();
//    Collection<? extends FlowStreamDefinition> flowStreams = definition.getFlowStreams();
//
//    /**
//     * Validates the streams based on the specification of connections
//     * in the flow definition
//     */
//    ImmutablePair<Boolean, String> schemaStatus = validateSchema(flowletInstances, flowStreams, connections);
//    success = schemaStatus.getFirst();
//    if (!success) {
//      return schemaStatus;
//    }
//    LOG.info("Validation of flow successful");
//    return new ImmutablePair<Boolean, String>(true, "OK");
//  }
}
