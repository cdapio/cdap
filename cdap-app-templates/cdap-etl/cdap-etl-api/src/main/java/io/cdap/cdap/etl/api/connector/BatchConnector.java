package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;

import java.util.List;

/**
 * Batch connector that relies on the {@link InputFormatProvider} to read from the resources
 *
 * @param <VAL_IN>
 */
public interface BatchConnector<VAL_IN> extends Connector {
  String PLUGIN_TYPE = "batchconnector";

  /**
   * Return the input format this connector will use to do the sampling
   */
  InputFormatProvider getInputFormatProvider();

  /**
   * Transform the sampled records back to StructuredRecord
   */
  List<StructuredRecord> transform(VAL_IN records);
}
