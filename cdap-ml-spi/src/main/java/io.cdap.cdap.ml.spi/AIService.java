
package io.cdap.cdap.ml.spi;

import io.cdap.cdap.proto.ApplicationDetail;

/**
 * Summarizes the given application details in mentioned format.
 */
public interface AIService {

     /**
      *
      * @param applicationDetail The detailed information about the application.
      * @param format The format in which the summary should be returned.
      * @return A summarized representation of the application details, formatted according to the specified format.
      */
     public String summarizeApp(ApplicationDetail applicationDetail, String format);
}
