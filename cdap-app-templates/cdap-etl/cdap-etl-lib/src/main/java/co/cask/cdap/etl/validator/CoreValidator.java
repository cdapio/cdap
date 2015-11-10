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

package co.cask.cdap.etl.validator;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.Validator;
import org.apache.commons.validator.GenericValidator;
import org.apache.commons.validator.routines.CreditCardValidator;
import org.apache.commons.validator.routines.DateValidator;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.DoubleValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.FloatValidator;
import org.apache.commons.validator.routines.ISBNValidator;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.commons.validator.routines.IntegerValidator;
import org.apache.commons.validator.routines.LongValidator;
import org.apache.commons.validator.routines.RegexValidator;
import org.apache.commons.validator.routines.ShortValidator;
import org.apache.commons.validator.routines.UrlValidator;

/**
 * Utility class containing validator functions provided by org.apache.commons.validator.routines package.
 */
@Plugin(type = "validator")
@Name("core")
@Description("Core Validator functions.")
public class CoreValidator implements Validator {

  private static final EmailValidator EMAIL_VALIDATOR = EmailValidator.getInstance();
  private static final DateValidator DATE_VALIDATOR = new DateValidator();
  private static final CreditCardValidator CREDIT_CARD_VALIDATOR = new CreditCardValidator();
  private static final DoubleValidator DOUBLE_VALIDATOR = new DoubleValidator();
  private static final IntegerValidator INTEGER_VALIDATOR = new IntegerValidator();
  private static final FloatValidator FLOAT_VALIDATOR = new FloatValidator();
  private static final ShortValidator SHORT_VALIDATOR = new ShortValidator();
  private static final LongValidator LONG_VALIDATOR  = new LongValidator();
  private static final UrlValidator URL_VALIDATOR  = new UrlValidator();
  private static final DomainValidator DOMAIN_VALIDATOR = DomainValidator.getInstance();
  private static final ISBNValidator ISBN_VALIDATOR = new ISBNValidator();
  private static final InetAddressValidator INET_ADDRESS_VALIDATOR = new InetAddressValidator();

  @Override
  public String getValidatorName() {
    return "coreValidator";
  }

  @Override
  public Object getValidator() {
    return new ValidatorUtil();
  }

  /**
   * Wrapper class for apache validator functions.
   */
  public static final class ValidatorUtil {

    /**
     * Checks if the passed param is a valid date.
     * uses {@link org.apache.commons.validator.routines.DateValidator} to check validity.
     */
    public boolean isDate(String date) {
      return DATE_VALIDATOR.isValid(date);
    }

    /**
     * Checks if the passed param is a valid CreditCard.
     * uses {@link org.apache.commons.validator.routines.CreditCardValidator} to check validity.
     */
    public boolean isCreditCard(String creditCard) {
      return CREDIT_CARD_VALIDATOR.isValid(creditCard);
    }

    /**
     * Checks if the input field is null, and if the length of the field is greater than zero, excluding whitespace.
     */
    public boolean isBlankOrNull(String input) {
      return GenericValidator.isBlankOrNull(input);
    }

    /**
     * Checks if the input fiels is an valid email address.
     */
    public boolean isEmail(String email) {
      return EMAIL_VALIDATOR.isValid(email);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     */
    public boolean isInRange(double value, double min , double max) {
      return DOUBLE_VALIDATOR.isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     */
    public boolean isInRange(int value, int min , int max) {
      return INTEGER_VALIDATOR.isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     */
    public boolean isInRange(float value, float min , float max) {
      return FLOAT_VALIDATOR.isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     */
    public boolean isInRange(short value, short min , short max) {
      return SHORT_VALIDATOR.isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     */
    public boolean isInRange(long value, long min , long max) {
      return LONG_VALIDATOR.isInRange(value, min, max);
    }

    /**
     * Checks if the value can safely be converted to a int primitive.
     */
    public boolean isInt(String input) {
      return GenericValidator.isInt(input);
    }

    /**
     * Checks if the value can safely be converted to a long primitive.
     */
    public boolean isLong(String input) {
      return GenericValidator.isLong(input);
    }

    /**
     * Checks if the value can safely be converted to a short primitive.
     */
    public boolean isShort(String input) {
      return GenericValidator.isShort(input);
    }

    /**
     * Checks if a field is a valid URL address.
     */
    public boolean isUrl(String url) {
      return URL_VALIDATOR.isValid(url);
    }

    /**
     * Checks if the value matches the regular expression.
     */
    public boolean matchRegex(String pattern, String input) {
      return new RegexValidator(pattern).isValid(input);
    }

    /**
     * Checks if the value's adjusted length is less than or equal to the max.
     */
    public boolean maxLength(String input, int maxLength) {
      return GenericValidator.maxLength(input, maxLength);
    }

    /**
     * Checks if the value is less than or equal to the max.
     */
    public boolean maxValue(double val, double maxVal) {
      return GenericValidator.maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     */
    public boolean maxValue(long val, long maxVal) {
      return GenericValidator.maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     */
    public boolean maxValue(int val, int maxVal) {
      return GenericValidator.maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     */
    public boolean maxValue(float val, float maxVal) {
      return GenericValidator.maxValue(val, maxVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     */
    public boolean minValue(double val, double minVal) {
      return GenericValidator.minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     */
    public boolean minValue(long val, long minVal) {
      return GenericValidator.minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     */
    public boolean minValue(int val, int minVal) {
      return GenericValidator.minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     */
    public boolean minValue(float val, float minVal) {
      return GenericValidator.minValue(val, minVal);
    }

    /**
     * Checks if the input value's adjusted length is greater than or equal to the minimum specified by length.
     */
    public boolean minLength(String input, int length) {
      return GenericValidator.minLength(input, length);
    }

    /**
     * Checks if the code is either a valid ISBN-10 or ISBN-13 code.
     */
    public boolean isValidISBN(String isbn) {
      return ISBN_VALIDATOR.isValid(isbn);
    }

    /**
     * Validates an IPv4 address.
     */
    public boolean isValidInet4Address(String ipv4) {
      return INET_ADDRESS_VALIDATOR.isValidInet4Address(ipv4);
    }

    /**
     * Validates an IPv6 address.
     */
    public boolean isValidInet6Address(String ipv6) {
      return INET_ADDRESS_VALIDATOR.isValidInet6Address(ipv6);
    }

    /**
     * Checks if the specified string is a valid IP address.
     */
    public boolean isValidIp(String ip) {
      return INET_ADDRESS_VALIDATOR.isValid(ip);
    }

    /**
     * Checks if the specified String matches an IANA-defined country code top-level domain.
     */
    public boolean isValidCountryCodeTid(String ccTld) {
      return DOMAIN_VALIDATOR.isValidCountryCodeTld(ccTld);
    }

    /**
     * Checks if the specified String matches an IANA-defined generic top-level domain.
     */
    public boolean isValidGenericTId(String gTld) {
      return DOMAIN_VALIDATOR.isValidGenericTld(gTld);
    }


    /**
     * Checks if the specified String matches an IANA-defined infrastructure top-level domain.
     */
    public boolean isValidInfrastructureTId(String iTld) {
      return DOMAIN_VALIDATOR.isValidInfrastructureTld(iTld);
    }

    /**
     * Checks if the specified String matches an widely used "local" domains (localhost or localdomain).
     */
    public boolean isValidLocalTId(String lTld) {
      return DOMAIN_VALIDATOR.isValidLocalTld(lTld);
    }


    /**
     * Checks if the specified String matches an IANA-defined top-level domain.
     */
    public boolean isValidTId(String tld) {
      return DOMAIN_VALIDATOR.isValidTld(tld);
    }
  }
}
