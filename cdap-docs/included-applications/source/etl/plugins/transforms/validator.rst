.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-transformations-validator:

==========================
Transformations: Validator
==========================

.. rubric:: Description

Validates a record, writing to an error dataset if the record is invalid.
Otherwise it passes the record on to the next stage.
  
The transform validates records using a custom Javascript function based on a set of 
:ref:`available validator functions <included-apps-etl-plugins-shared-core-validator>` in the 
:ref:`CoreValidator <included-apps-etl-plugins-shared-core-validator>`.

.. rubric:: Use Case

The transform is used when you need to validate records. For example, you may want to
validate records as being valid IP addresses or valid dates and log errors if they aren't
valid.

.. rubric:: Properties

**validators** Comma-separated list of validators that are used by the validationScript.
Example: ``"validators": "core"``

**validationScript:** Javascript that must implement a function ``isValid`` that takes a JSON object
representation of the input record, and returns a result JSON.
The returned JSON will include these fields; ``errorCode`` and ``errorMsg`` can be ignored for valid records::

  {
    "isValid" : true [or] false,
    "errorCode" : number [should be an valid integer],
    "errorMsg" : "Message indicating the error and why the record failed validation"
  }

.. rubric:: Examples

::

      {
        "name": "Validator",
        "properties": {
          "validators": "core",
          "validationScript": "function isValid(input) {
                                  if (!coreValidator.maxLength(input.body, 10))
                                    {
                                      return {'isValid': false, 'errorCode': 10,
                                              'errorMsg': 'body length greater than 10'};
                                    }
                                  return {'isValid' : true};
                                };"
        }
      }
      
This example sends an error code ``'10'`` for any records whose ``'body'`` field contains
a value whose length is greater than 10. It has been "pretty-printed" for readability. It
uses the :ref:`CoreValidator <included-apps-etl-plugins-shared-core-validator>` (included
using ``"validators": "core"``) and references a function using its Javascript name
(``coreValidator.maxLength``).

::

      {
        "name": "Validator",
        "properties": {
          "validators": "core",
          "validationScript": "function isValid(input) {
                                  var isValid = true;
                                  var errMsg = \"\";
                                  var errCode = 0;
                                  if (!coreValidator.isDate(input.date)) {
                                     isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;
                                  } else if (!coreValidator.isUrl(input.url)) { 
                                     isValid = false; errMsg = \"invalid url\"; errCode = 7;
                                  } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {
                                     isValid = false; errMsg = \"content length >1MB\"; errCode = 10;
                                  }
                                  return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg};
                                };"
        }
      }

This example sends an error code ``'5'`` for any records whose ``'date'`` field is an
invalid date, sends an error code ``'7'`` for any records whose ``'url'`` field is an
invalid URL, and sends an error code ``'10'`` for any records whose ``'content_length'``
field is greater than 1MB.;

It has been "pretty-printed" for readability. It uses the
:ref:`CoreValidator <included-apps-etl-plugins-shared-core-validator>` (included using
``"validators": "core"``) and references functions using their Javascript names (such as
``coreValidator.isDate``).
