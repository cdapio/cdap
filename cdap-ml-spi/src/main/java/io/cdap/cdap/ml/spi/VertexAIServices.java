package io.cdap.cdap.ml.spi;

import java.io.IOException;

/**
 *
 */
public interface VertexAIServices {

  public String summarizePipeline(String architecture,
      String format) throws IOException;
}
