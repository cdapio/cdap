package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;

import java.util.List;

/**
 * Connector that connects to the resources directly and fetch the results back
 */
public interface DirectConnector extends Connector {
  String PLUGIN_TYPE = "directconnector";

  List<StructuredRecord> sample(SampleRequest sampleRequest);
}
