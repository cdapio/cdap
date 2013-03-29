package com.continuuity.machinedata;

import au.com.bytecode.opencsv.CSVParser;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.machinedata.data.CPUStat;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CPUStatsParserFlowlet extends AbstractFlowlet {

  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsParserFlowlet.class);

  public static final String NAME = "CPU_STATS_PARSER_FLOWLET";
  public static final String DESC = "CPU stats parser flowlet";
  public static final String OUTPUT_NAME = "parserOutput";

  @Output(OUTPUT_NAME)
  OutputEmitter<CPUStat> parserOutput;

  private final CSVParser csvParser = new CSVParser(',', '"', '\\', false);

  @ProcessInput
  public void process(StreamEvent event) throws OperationException {

    if (event == null) {
      return;
    }

      String eventString = new String(Bytes.toBytes(event.getBody()));

      if (eventString == null ) {
        //throw new OperationException(StatusCode.INTERNAL_ERROR, "Unable to parse Stream data");
      }

      // Parse as CSV
      String[] parsed = null;
      try {
        parsed = this.csvParser.parseLine(eventString);
        if (parsed.length != 3) {
          //throw new IOException();
        }
        else {
          parserOutput.emit(new CPUStat(/*ts*/ Long.parseLong(parsed[0]),/*cpu*/ parsed[1],/*hostname*/ parsed[2]));
        }
      } catch (IOException e) {
        LOG.error("Error parsing log line: " + eventString + "expect 3 params, got:" + parsed.length);
        //throw new RuntimeException("Invalid input string: " + eventString);
      }
  }
}