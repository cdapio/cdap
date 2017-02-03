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

import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.joran.action.Action;
import ch.qos.logback.core.joran.action.ActionConst;
import ch.qos.logback.core.joran.spi.ActionException;
import ch.qos.logback.core.joran.spi.InterpretationContext;
import ch.qos.logback.core.joran.spi.Pattern;
import co.cask.cdap.logging.appender.FlushableAppender;
import org.xml.sax.Attributes;

import java.util.Map;

/**
 * A {@link JoranConfigurator} for parsing logback xml configuration for log processing pipeline.
 */
public class PipelineConfigurator extends JoranConfigurator {

  @Override
  protected void buildInterpreter() {
    super.buildInterpreter();
    interpreter.getRuleStore()
      .addRule(new Pattern("configuration/appender"), new WrapAppenderAction<ILoggingEvent>());
  }

  /**
   * An {@link Action} that wraps {@link Appender} with {@link FlushableAppender}.
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
      appenderBag.put(appenderName, new FlushableAppender<>(appender));
    }

    @Override
    public void end(InterpretationContext ec, String name) throws ActionException {
      // no-op
    }
  }
}
