/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common.kafka;

import org.apache.hadoop.io.Text;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Exception Writable.
 */
public class ExceptionWritable extends Text {

  public ExceptionWritable() {
    super();
  }

  public ExceptionWritable(String exception) {
    super(exception);
  }

  public ExceptionWritable(Exception e) {
    set(null, e);
  }

  public ExceptionWritable(String message, Exception e) {
    set(message, e);
  }

  public void set(String message, Throwable e) {
    StringWriter strWriter = new StringWriter();
    PrintWriter printer = new PrintWriter(strWriter);

    if (message != null) {
      printer.write(message);
      printer.write("\n");
    }

    e.printStackTrace(printer);

    super.set(strWriter.toString());
    printer.close();
  }

  public void set(Exception e) {
    set(null, e);
  }

  public void set(ExceptionWritable other) {
    super.set(other.getBytes());
  }
}
