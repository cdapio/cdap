.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-etl-transformations-validator:

==========================
Transformations: Validator
==========================

.. rubric:: Description

A transform that validates records using a custom Javascript function based on a set of 
:ref:`available validator functions <apptemplates-etl-validators-corevalidator>` in the 
:ref:`CoreValidator <apptemplates-etl-validators-corevalidator>`.

.. rubric:: Use Case

The transform is used when you need to validate records. For example, you may want to
validate records as being valid IP addresses or valid dates and log errors if they aren't
valid.

.. rubric:: Properties

**validators** Comma-separated list of validators that are used by the validationScript.
Example: ``"validators": "core"``

**validationScript:** Javascript that implements the function ``isValid``, taking a JSON object
representation of the input record, and returning a JSON representing the result.
The returned JSON will include these fields; errorCode and errorMsg can be ignored for valid records::

  {
    "isValid" : true [or] false,
    "errorCode" : number [should be an valid integer],
    "errorMsg" : "Message indicating the error and why the record failed validation"
  }

.. rubric:: Example

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
      
This example sends an error code "10" for any records whose 'body' field contains a value
whose length is greater than 10. It has been "pretty-printed" for readability. It uses the
:ref:`CoreValidator <apptemplates-etl-validators-corevalidator>` (included using
``"validators": "core"``) and references a function using its Javascript name
(``coreValidator.maxLength``).
