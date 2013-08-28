/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.testsuite.testbatch;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.FileDataSet;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * This flowlet reads events from a stream and parses them as sentences of the form
 * <pre><name> bought <n> <items> for $<price></pre>. The event is then converted into
 * a Purchase object and emitted. If the event does not have this form, it is dropped.
 */
public class FileLoader extends AbstractFlowlet {

  /**
   *
   */
  OutputEmitter<FileStat> out;

  @UseDataSet("trx-txt")
  private FileDataSet fileDataSet;


  /**
   * Loads File Dataset on command and build filestat object, passes to the writer
   * @param event
   */
  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());

    try {
      if (body.startsWith("load")) {
        // Load file Dataset
        InputStream inputStream = fileDataSet.getInputStream();
        BufferedReader br = null;
        String line;
        try {
          br = new BufferedReader(new InputStreamReader(inputStream));
          while ((line = br.readLine()) != null) {
            FileStat fileStat = new FileStat(fileDataSet.getPath().toString(), line);
            out.emit(fileStat);
          }

        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (br != null) {
            try {
              br.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
