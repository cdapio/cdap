/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.logging.pipeline;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.model.ContextNameModel;
import ch.qos.logback.classic.model.processor.ContextNameModelHandler;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.joran.JoranConstants;
import ch.qos.logback.core.model.AppenderModel;
import ch.qos.logback.core.model.Model;
import ch.qos.logback.core.model.processor.AppenderModelHandler;
import ch.qos.logback.core.model.processor.DefaultProcessor;
import ch.qos.logback.core.model.processor.DependencyDefinition;
import ch.qos.logback.core.model.processor.ModelHandlerException;
import ch.qos.logback.core.model.processor.ModelInterpretationContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Syncable;
import io.cdap.cdap.logging.appender.ForwardingAppender;
import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import org.slf4j.MDC;
import org.slf4j.spi.MDCAdapter;

/**
 * A {@link JoranConfigurator} for parsing logback xml configuration for log processing pipeline.
 */
public class LogPipelineConfigurator extends JoranConfigurator {

  private static final String PIPELINE_CONFIG_PREFIX = "log.pipeline";
  private final CConfiguration cConf;

  public LogPipelineConfigurator(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void setContext(Context context) {
    super.setContext(context);
    if (context instanceof LoggerContext) {
      LoggerContext loggerContext = (LoggerContext) context;
      if (loggerContext.getMDCAdapter() == null) {
        MDCAdapter adapter = MDC.getMDCAdapter();
        loggerContext.setMDCAdapter(
            adapter instanceof LogbackMDCAdapter ? adapter : new LogbackMDCAdapter());
      }
    }
  }

  @Override
  protected JoranConfigurator makeAnotherInstance() {
    LogPipelineConfigurator configurator = new LogPipelineConfigurator(cConf);
    configurator.setContext(context);
    return configurator;
  }

  @Override
  protected void addModelHandlerAssociations(DefaultProcessor defaultProcessor) {
    super.addModelHandlerAssociations(defaultProcessor);
    defaultProcessor.addHandler(ContextNameModel.class,
        (handlerContext, mic) -> new ContextConfigModelHandler(handlerContext, cConf));
    defaultProcessor.addHandler(AppenderModel.class,
        (handlerContext, mic) -> new WrapAppenderModelHandler<ILoggingEvent>(handlerContext));
  }

  /**
   * A model handler that sets the context name and copies properties from {@link CConfiguration} to
   * the {@link LoggerContext} based on that context name.
   */
  private static final class ContextConfigModelHandler extends ContextNameModelHandler {

    private final CConfiguration cConf;

    ContextConfigModelHandler(Context context, CConfiguration cConf) {
      super(context);
      this.cConf = cConf;
    }

    @Override
    public void handle(ModelInterpretationContext mic, Model model) throws ModelHandlerException {
      super.handle(mic, model);
      String name = context.getName();
      if (name == null) {
        return;
      }
      String prefix = PIPELINE_CONFIG_PREFIX + "." + name + ".";

      for (Map.Entry<String, String> entry : cConf) {
        String key = entry.getKey();
        if (key.startsWith(prefix)) {
          context.putProperty(key.substring(prefix.length()), entry.getValue());
        }
      }
    }
  }

  /**
   * An {@link AppenderModelHandler} that wraps created {@link Appender} instances with
   * {@link WrappedAppender} for flushing and syncing.
   *
   * @param <E> type of the event used by the appender
   */
  private static final class WrapAppenderModelHandler<E> extends AppenderModelHandler<E> {

    WrapAppenderModelHandler(Context context) {
      super(context);
    }

    @Override
    public void handle(ModelInterpretationContext mic, Model model) throws ModelHandlerException {
      // Logback 1.3+ skips appenders that are not referenced, while Logback 1.2 created and
      // started every declared appender, so errors in unused appenders failed validation.
      // Mark every appender as referenced to keep that behavior.
      String appenderName = mic.subst(((AppenderModel) model).getName());
      if (!mic.hasDependers(appenderName)) {
        mic.addDependencyDefinition(new DependencyDefinition(new Model(), appenderName));
      }
      super.handle(mic, model);
    }

    @Override
    public void postHandle(ModelInterpretationContext mic, Model model)
        throws ModelHandlerException {
      AppenderModel appenderModel = (AppenderModel) model;
      try {
        super.postHandle(mic, model);
      } catch (RuntimeException e) {
        addError("Failed to configure appender " + appenderModel.getName(), e);
        return;
      }
      if (appenderModel.isSkipped()) {
        return;
      }

      String appenderName = mic.subst(appenderModel.getName());
      // Safe by specification: Logback populates APPENDER_BAG as Map<String, Appender<E>>.
      @SuppressWarnings("unchecked")
      Map<String, Appender<E>> appenderBag =
          (Map<String, Appender<E>>) mic.getObjectMap().get(JoranConstants.APPENDER_BAG);
      if (appenderBag == null) {
        return;
      }

      Appender<E> created = appenderBag.get(appenderName);
      if (created != null && !(created instanceof WrappedAppender)) {
        appenderBag.put(appenderName, new WrappedAppender<>(created));
      }
    }
  }

  /**
   * An {@link Appender} that implements both {@link Flushable} and {@link Syncable}. If either
   * {@link #flush()} or {@link #sync()} failed, it will be retried on the next {@link
   * #doAppend(Object)} call.
   *
   * @param <E> type of event that can be appended.
   */
  private static final class WrappedAppender<E> extends ForwardingAppender<E> implements Flushable,
      Syncable {

    private boolean needFlush;
    private boolean needSync;

    WrappedAppender(Appender<E> delegate) {
      super(delegate);
    }

    @Override
    public void doAppend(E event) throws LogbackException {
      if (needSync) {
        try {
          sync();
        } catch (IOException e) {
          throw new LogbackException("Sync failed. Cannot append event.", e);
        }
      }
      if (needFlush) {
        try {
          flush();
        } catch (IOException e) {
          throw new LogbackException("Flush failed. Cannot append event.", e);
        }
      }
      super.doAppend(event);
    }

    @Override
    public void flush() throws IOException {
      Appender<E> appender = getDelegate();
      if (appender instanceof Flushable) {
        try {
          ((Flushable) appender).flush();
          needFlush = false;
        } catch (Exception e) {
          needFlush = true;
          throw e;
        }
      }
    }

    @Override
    public void sync() throws IOException {
      flush();

      Appender<E> appender = getDelegate();
      if (appender instanceof Syncable) {
        try {
          ((Syncable) appender).sync();
          needSync = false;
        } catch (Exception e) {
          needSync = true;
          throw e;
        }
      }
    }
  }
}
