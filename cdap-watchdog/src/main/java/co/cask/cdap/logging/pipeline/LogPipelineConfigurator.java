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

package co.cask.cdap.logging.pipeline;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.joran.action.ContextNameAction;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.joran.action.Action;
import ch.qos.logback.core.joran.action.ActionConst;
import ch.qos.logback.core.joran.spi.ActionException;
import ch.qos.logback.core.joran.spi.InterpretationContext;
import ch.qos.logback.core.joran.spi.Pattern;
import ch.qos.logback.core.joran.spi.RuleStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Syncable;
import co.cask.cdap.logging.appender.ForwardingAppender;
import org.xml.sax.Attributes;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;

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
  protected void buildInterpreter() {
    super.buildInterpreter();
    RuleStore ruleStore = interpreter.getRuleStore();
    ruleStore.addRule(new Pattern("configuration/contextName"), new ContextConfigAction(cConf));
    ruleStore.addRule(new Pattern("configuration/appender"), new WrapAppenderAction<ILoggingEvent>());
  }

  /**
   * An action that copies properties from {@link CConfiguration} to the {@link LoggerContext}
   * based on the context name.
   */
  private static final class ContextConfigAction extends ContextNameAction {

    private final CConfiguration cConf;

    ContextConfigAction(CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Override
    public void body(InterpretationContext ec, String body) {
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
   * An {@link Action} that wraps {@link Appender} with {@link WrappedAppender} for flushing and syncing.
   *
   * @param <E> type of the event used by the appender
   */
  private static final class WrapAppenderAction<E> extends Action {

    @Override
    public void begin(InterpretationContext ec, String name, Attributes attributes) throws ActionException {
      String appenderName = ec.subst(attributes.getValue(NAME_ATTRIBUTE));

      // The execution context contains a bag which contains the appenders created thus far.
      @SuppressWarnings("unchecked")
      Map<String, Appender<E>> appenderBag = (Map<String, Appender<E>>) ec.getObjectMap().get(ActionConst.APPENDER_BAG);

      Appender<E> appender = appenderBag.get(appenderName);
      appenderBag.put(appenderName, new WrappedAppender<>(appender));
    }

    @Override
    public void end(InterpretationContext ec, String name) throws ActionException {
      // no-op
    }
  }

  /**
   * An {@link Appender} that implements both {@link Flushable} and {@link Syncable}. If either
   * {@link #flush()} or {@link #sync()} failed, it will be retried on the next {@link #doAppend(Object)}
   * call.
   *
   * @param <E> type of event that can be appended.
   */
  private static final class WrappedAppender<E> extends ForwardingAppender<E> implements Flushable, Syncable {

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
