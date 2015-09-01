.. meta::
:author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==========================
Transformations: Validator
==========================

.. rubric:: Description

A transform that validates records using a custom Javascript function based on a set of available validator functions.

.. rubric:: Use Case

The transform is used when you need to validate records.
For example, you may want to validate records as being valid IP address or valid dates and log errors if they aren't valid.

.. rubric:: Properties

**validators** Comma separated list of validators that are used by the validationScript. Example: "validators" : "core"

**validationScript:** Javascript that implements a function 'isValid', taking a JSON object
representation of the input record, and returning a Map<String, String> representing the result.
the map should have the following fields

::

  {
    "isValid" : "true (or) false",
    "errorCode" : "number",
    "errorMsg" : "message indicating the error and why the record failed validation"
  }


.. rubric:: Example

::

      {
        "name": "Validator",
        "properties": {
          "validators": "core",
          "validationScript": "function isValid(input) {
                                  input = JSON.parse(input);
                                  var resultMap = new java.util.HashMap();
                                  resultMap.put('errorCode', '0');
                                  resultMap.put('errorMsg', '');
                                  resultMap.put('isValid', 'true');
                                  if (!coreValidator.maxLength(input.body, 10))
                                    {
                                      resultMap.put('isValid', 'false');
                                      resultMap.put('errorCode', 10);
                                      resultMap.put('errorMsg', input.body);
                                    }
                                  return resultMap;
                                };"
        }
      }
This example sends error code "10" for any records whose 'body' field contains a value whose length is greater than 10.
