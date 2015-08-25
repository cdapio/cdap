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

package co.cask.cdap.template.etl.validator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.template.etl.api.Validator;
import org.apache.commons.validator.routines.CreditCardValidator;
import org.apache.commons.validator.routines.DateValidator;

/**
 * Utility class containing validator functions provided by org.apache.commons.validator.routines package.
 */
@Plugin(type = "validator")
@Name("Apache")
@Description("Apache validator functions from org.apache.commons.validator.routines package. " +
  "Includes isDate(date), isCreditCard(creditCard) methods.")
public class ApacheValidator implements Validator {

  @Override
  public Object getValidator() {
    return new ApacheValidatorWrapper();
  }

  /**
   * Wrapper class for apache validator functions.
   */
  public static final class ApacheValidatorWrapper {

    /**
     * return true if the passed param is a valid date.
     * uses {@link org.apache.commons.validator.routines.DateValidator} to check validity.
     * @param date
     * @return boolean
     */
    public static boolean isDate(String date) {
      return new DateValidator().isValid(date);
    }

    /**
     * return true if the passed param is a valid CreditCard.
     * uses {@link org.apache.commons.validator.routines.CreditCardValidator} to check validity.
     * @param creditCard
     * @return boolean
     */
    public static boolean isCreditCard(String creditCard) {
      return new CreditCardValidator().isValid(creditCard);
    }
  }
}
