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
@Name("apache")
@Description("Core Validator functions.")
public class ApacheValidator implements Validator {

  @Override
  public String getValidatorVariableName() {
    return "apacheValidator";
  }

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

    /**
     * Checks if the field is null and length of the field is greater than zero not including whitespace.
     * @return
     */
    public static boolean isBlankOrNull(String input) {
      return new GenericValidator().isBlankOrNull(input);
    }

    /**
     * Checks if a field has a valid e-mail address.
     * @param email
     * @return
     */
    public static boolean isEmail(String email) {
      return EmailValidator.getInstance().isValid(email);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     * @param value
     * @param min
     * @param max
     * @return
     */
    public static boolean isInRange(double value, double min , double max) {
      return new DoubleValidator().isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     * @param value
     * @param min
     * @param max
     * @return
     */
    public static boolean isInRange(int value, int min , int max) {
      return new IntegerValidator().isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     * @param value
     * @param min
     * @param max
     * @return
     */
    public static boolean isInRange(float value, float min , float max) {
      return new FloatValidator().isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     * @param value
     * @param min
     * @param max
     * @return
     */
    public static boolean isInRange(short value, short min , short max) {
      return new ShortValidator().isInRange(value, min, max);
    }

    /**
     * Checks if a value is within a range (min & max specified in the vars attribute).
     * @param value
     * @param min
     * @param max
     * @return
     */
    public static boolean isInRange(long value, long min , long max) {
      return new LongValidator().isInRange(value, min, max);
    }

    /**
     * Checks if the value can safely be converted to a int primitive.
     * @param input
     * @return
     */
    public static boolean isInt(String input) {
      return new GenericValidator().isInt(input);
    }

    /**
     * Checks if the value can safely be converted to a long primitive.
     * @param input
     * @return
     */
    public static boolean isLong(String input) {
      return new GenericValidator().isLong(input);
    }

    /**
     * Checks if the value can safely be converted to a short primitive.
     * @param input
     * @return
     */
    public static boolean isShort(String input) {
      return new GenericValidator().isShort(input);
    }

    /**
     * Checks if a field is a valid URL address.
     * @param url
     * @return
     */
    public static boolean isUrl(String url) {
      return new UrlValidator().isValid(url);
    }

    /**
     * Checks if the value matches the regular expression.
     * @param pattern
     * @param input
     * @return
     */
    public static boolean matchRegex(String pattern, String input) {
      return new RegexValidator(pattern).isValid(input);
    }

    /**
     * Checks if the value's adjusted length is less than or equal to the max.
     * @param input
     * @param maxLength
     * @return
     */
    public static boolean maxLength(String input, int maxLength) {
      return new GenericValidator().maxLength(input, maxLength);
    }

    /**
     * Checks if the value is less than or equal to the max.
     * @param val
     * @param maxVal
     * @return
     */
    public static boolean maxValue(double val, double maxVal) {
      return new GenericValidator().maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     * @param val
     * @param maxVal
     * @return
     */
    public static boolean maxValue(long val, long maxVal) {
      return new GenericValidator().maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     * @param val
     * @param maxVal
     * @return
     */
    public static boolean maxValue(int val, int maxVal) {
      return new GenericValidator().maxValue(val, maxVal);
    }

    /**
     * Checks if the value is less than or equal to the max.
     * @param val
     * @param maxVal
     * @return
     */
    public static boolean maxValue(float val, float maxVal) {
      return new GenericValidator().maxValue(val, maxVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     * @param val
     * @param minVal
     * @return
     */
    public static boolean minValue(double val, double minVal) {
      return new GenericValidator().minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     * @param val
     * @param minVal
     * @return
     */
    public static boolean minValue(long val, long minVal) {
      return new GenericValidator().minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     * @param val
     * @param minVal
     * @return
     */
    public static boolean minValue(int val, int minVal) {
      return new GenericValidator().minValue(val, minVal);
    }

    /**
     * Checks if the value is greater than or equal to the min.
     * @param val
     * @param minVal
     * @return
     */
    public static boolean minValue(float val, float minVal) {
      return new GenericValidator().minValue(val, minVal);
    }

    /**
     * Checks if the value's adjusted length is greater than or equal to the min.
     * @param input
     * @param length
     * @return
     */
    public static boolean minLength(String input, int length) {
      return new GenericValidator().minLength(input, length);
    }

    /**
     * Checks if the code is either a valid ISBN-10 or ISBN-13 code.
     * @param isbn
     */
    public static boolean isValidISBN(String isbn) {
      return new ISBNValidator().isValid(isbn);
    }

    /**
     * Validates an IPv4 address.
     * @param ipv4
     */
    public static boolean isValidInet4Address(String ipv4) {
      return new InetAddressValidator().isValidInet4Address(ipv4);
    }

    /**
     * Validates an IPv6 address.
     * @param ipv6
     */
    public static boolean isValidInet6Address(String ipv6) {
      return new InetAddressValidator().isValidInet6Address(ipv6);
    }

    /**
     * Checks if the specified string is a valid IP address.
     * @param ip
     */
    public static boolean isValidIp(String ip) {
      return new InetAddressValidator().isValid(ip);
    }

    /**
     * Returns true if the specified String matches any IANA-defined country code top-level domain.
     * @param ccTld
     * @return
     */
    public static boolean isValidCountryCodeTid(String ccTld) {
      return DomainValidator.getInstance().isValidCountryCodeTld(ccTld);
    }

    /**
     * Returns true if the specified String matches any IANA-defined generic top-level domain.
     * @param gTld
     * @return
     */
    public static boolean isValidGenericTId(String gTld) {
      return DomainValidator.getInstance().isValidGenericTld(gTld);
    }


    /**
     * Returns true if the specified String matches any IANA-defined infrastructure top-level domain.
     * @param iTld
     * @return
     */
    public static boolean isValidInfrastructureTId(String iTld) {
      return DomainValidator.getInstance().isValidInfrastructureTld(iTld);
    }

    /**
     * Returns true if the specified String matches any widely used "local" domains (localhost or localdomain).
     * @param lTld
     * @return
     */
    public static boolean isValidLocalTId(String lTld) {
      return DomainValidator.getInstance().isValidLocalTld(lTld);
    }


    /**
     * Returns true if the specified String matches any IANA-defined top-level domain.
     * @param tld
     * @return
     */
    public static boolean isValidTId(String tld) {
      return DomainValidator.getInstance().isValidTld(tld);
    }
  }
}
