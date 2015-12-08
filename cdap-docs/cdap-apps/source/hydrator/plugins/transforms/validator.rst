.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-transformations-validator:

==========================
Transformations: Validator
==========================

.. rubric:: Description

Validates a record, writing to an error dataset if the record is invalid.
Otherwise it passes the record on to the next stage.

The transform validates records using a custom Javascript function based on a set of
:ref:`available validator functions <cdap-apps-etl-plugins-shared-core-validator>` in the
:ref:`CoreValidator <cdap-apps-etl-plugins-shared-core-validator>`.

.. rubric:: Use Case

The transform is used when you need to validate records. For example, you may want to
validate records as being valid IP addresses or valid dates and log errors if they aren't
valid.

.. rubric:: Properties

**validators** Comma-separated list of validators that are used by the validationScript.
Example: ``"validators": "core"``

**validationScript:** Javascript that must implement a function ``isValid`` that takes a JSON object
(representing the input record) and a context object (encapsulating CDAP metrics, logger, and validators)
and returns a result JSON with validity, error code, and error message.
Example response::

  {
    "isValid" : true [or] false,
    "errorCode" : number [should be an valid integer],
    "errorMsg" : "Message indicating the error and why the record failed validation"
  }

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.

.. rubric:: Examples

::

      {
        "name": "Validator",
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

This example sends an error code ``'10'`` for any records whose ``'body'`` field contains
a value whose length is greater than 10. It has been "pretty-printed" for readability. It
uses the :ref:`CoreValidator <cdap-apps-etl-plugins-shared-core-validator>` (included
using ``"validators": "core"``) and references a function using its Javascript name
(``coreValidator.maxLength``).

::

      {
        "name": "Validator",
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

This example uses the key-value dataset ``'blacklist'`` as a lookup table,
and sends an error code ``'10'`` for any records whose ``'body'`` field exists in the ``'blacklist'`` dataset.
It has been "pretty-printed" for readability.

::

      {
        "name": "Validator",
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

This example sends an error code ``'5'`` for any records whose ``'date'`` field is an
invalid date, sends an error code ``'7'`` for any records whose ``'url'`` field is an
invalid URL, and sends an error code ``'10'`` for any records whose ``'content_length'``
field is greater than 1MB.

It has been "pretty-printed" for readability. It uses the
:ref:`CoreValidator <cdap-apps-etl-plugins-shared-core-validator>` (included using
``"validators": "core"``) and references functions using their Javascript names (such as
``coreValidator.isDate``).

**Note:** These default metrics are emitted by this transform:

.. csv-table::
   :header: "Metric Name","Description"
   :widths: 40,60

   "``records.in``","Input records processed by this transform stage"
   "``records.out``","Output records sent to the next stage"
   "``invalid``","Input records invalidated at this stage"
