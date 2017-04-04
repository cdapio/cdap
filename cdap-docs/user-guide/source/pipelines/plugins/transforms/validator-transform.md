# Validator


Description
-----------
Validates a record, writing to an error dataset if the record is invalid.
Otherwise it passes the record on to the next stage.

This table lists the methods available in CoreValidator that can be called from the ValidatorTransform:

    +==================================================================================================================+
    | function                                        | description                                                    |
    +==================================================================================================================+
    | isDate(String date)                             | Returns true if the passed param is a valid date               |
    | isCreditCard(String card)                       | Returns true if the passed param is a valid CreditCard         |
    | isBlankOrNull(String val)                       | Checks if the field is null and length of the field is greater |
    |                                                 | than zero not including whitespace                             |
    | isEmail(String email)                           | Checks if a field has a valid e-mail address                   |
    | isInRange(double value, double min, double max) | Checks if a value is within a range                            |
    | isInRange(int value, int min, int max)          | Checks if a value is within a range                            |
    | isInRange(float value, float min, float max)    | Checks if a value is within a range                            |
    | isInRange(short value, short min, short max)    | Checks if a value is within a range                            |
    | isInRange(long value, long min, long max)       | Checks if a value is within a range                            |
    | isInt(String input)                             | Checks if the value can be converted to a int primitive        |
    | isLong(String input)                            | Checks if the value can be converted to a long primitive       |
    | isShort(String input)                           | Checks if the value can be converted to a short primitive      |
    | isUrl(String input)                             | Checks if the value can be converted to a int primitive        |
    | matchRegex(String pattern, String input)        | Checks if the value matches the regular expression             |
    | maxLength(String input, int maxLength)          | Checks if the value length is less than or equal to the max    |
    | maxValue(double val, double maxVal)             | Checks if the value is less than or equal to the max           |
    | maxValue(long val, long maxVal)                 | Checks if the value is less than or equal to the max           |
    | maxValue(int val, int maxVal)                   | Checks if the value is less than or equal to the max           |
    | maxValue(float val, float maxVal)               | Checks if the value is less than or equal to the max           |
    | minValue(double val, double minVal)             | Checks if the value is greater than or equal to the min        |
    | minValue(long val, long minVal)                 | Checks if the value is greater than or equal to the min        |
    | minValue(int val, int minVal)                   | Checks if the value is greater than or equal to the min        |
    | minValue(float val, float minVal)               | Checks if the value is greater than or equal to the min        |
    | minLength(String input, int length)             | Checks if the value length is greater than or equal to the min |
    | isValidISBN(String isbn)                        | Checks if the code is either a valid ISBN-10 or ISBN-13 code   |
    | isValidInet4Address(String ipv4)                | Validates an IPv4 address                                      |
    | isValidInet6Address(String ipv6)                | Validates an IPv6 address                                      |
    | isValidIp(String ip)                            | Checks if the specified string is a valid IP address           |
    | isValidCountryCodeTid(String ccTld)             | Returns true if the input matches any IANA-defined             |
    |                                                 | country code top-level domain                                  |
    | isValidGenericTId(String gTld)                  | Returns true if the input matches any IANA-defined             |
    |                                                 | generic top-level domain                                       |
    | isValidInfrastructureTId(String iTld)           | Returns true if the input matches any IANA-defined             |
    |                                                 | infrastructure top-level domain                                |
    | isValidLocalTId(String lTld)                    | Returns true if the input matches any widely used              |
    |                                                 | local domains (localhost or localdomain)                       |
    | isValidTId(String tld)                          | Returns true if the input matches any IANA-defined             |
    |                                                 | top-level domain                                               |
    +==================================================================================================================+


Use Case
--------
The transform is used when you need to validate records. For example, you may want to
validate records as being valid IP addresses or valid dates and log errors if they aren't
valid.


Properties
----------
**validators** Comma-separated list of validators that are used by the validationScript.
Example: ``"validators": "core"``

**validationScript:** JavaScript that must implement a function ``isValid`` that takes a JSON object
(representing the input record) and a context object (encapsulating CDAP metrics, logger, and validators)
and returns a result JSON with validity, error code, and error message.
Example response:

    {
        "isValid" : true [or] false,
        "errorCode" : number [should be an valid integer],
        "errorMsg" : "Message indicating the error and why the record failed validation"
    }

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.


Examples
--------
This example sends an error code ``'10'`` for any records whose ``'body'`` field contains
a value whose length is greater than 10. It has been "pretty-printed" for readability. It
uses the ``CoreValidator`` (included using ``"validators": "core"`` ) and references a
function using its JavaScript name (``coreValidator.maxLength``):

    {
        "name": "Validator",
        "type": "transform",
        "properties": {
            "validators": "core",
            "validationScript": "function isValid(input, context) {
                                  var coreValidator = context.getValidator(\"coreValidator\");
                                  if (!coreValidator.maxLength(input.body, 10))
                                    {
                                      return {'isValid': false, 'errorCode': 10,
                                              'errorMsg': \"body length greater than 10\"};
                                    }
                                  return {'isValid' : true};
                                };"
        }
    }

This example uses the key-value dataset ``'blacklist'`` as a lookup table, and sends an
error code ``'10'`` for any records whose ``'body'`` field exists in the ``'blacklist'``
dataset. It has been "pretty-printed" for readability:

    {
        "name": "Validator",
        "type": "transform",
        "properties": {
            "lookup": "{
                \"blacklist\":{
                    \"type\":\"DATASET\"
                }
            }"
            "validationScript": "function isValid(input, context) {
                                  if (context.getLookup('blacklist').lookup(input.body) !== null)
                                    {
                                      return {'isValid': false, 'errorCode': 10,
                                              'errorMsg': \"input blacklisted\"};
                                    }
                                  return {'isValid' : true};
                                };"
        }
    }

This example sends an error code ``'5'`` for any records whose ``'date'`` field is an
invalid date, sends an error code ``'7'`` for any records whose ``'url'`` field is an
invalid URL, and sends an error code ``'10'`` for any records whose ``'content_length'``
field is greater than 1MB.

It has been "pretty-printed" for readability. It uses the CoreValidator and references functions 
using their JavaScript names (such as ``coreValidator.isDate``):

    {
        "name": "Validator",
        "type": "transform",
        "properties": {
            "validators": "core",
            "validationScript": "function isValid(input, context) {
                                  var isValid = true;
                                  var errMsg = \"\";
                                  var errCode = 0;
                                  var coreValidator = context.getValidator(\"coreValidator\");
                                  var metrics = context.getMetrics();
                                  var logger = context.getLogger();
                                  if (!coreValidator.isDate(input.date)) {
                                     isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;
                                     metrics.count(\"invalid.date\", 1);
                                  } else if (!coreValidator.isUrl(input.url)) {
                                     isValid = false; errMsg = \"invalid url\"; errCode = 7;
                                     metrics.count(\"invalid.url\", 1);
                                  } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {
                                     isValid = false; errMsg = \"content length >1MB\"; errCode = 10;
                                     metrics.count(\"invalid.body.size\", 1);
                                  }
                                  if (!isValid) {
                                    logger.warn(\"Validation failed for record {}\", input);
                                  }
                                  return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg};
                                };"
        }
    }

**Note:** This plugin emits a metric called 'invalid' that tracks how many invalid records were found.

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
