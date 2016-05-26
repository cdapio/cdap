/*
 * Copyright © 2016 Cask Data, Inc.
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

function ValidatorFactory (esprima) {
  'ngInject';

  var AND = true,
      OR = false;

  function initValidationFields(properties, functionMap) {
    if (!properties.validationScript) { return {}; }

    var jsonTree = esprima.parse(properties.validationScript);

    /**
     *  Algorithm:
     *    1. Find the first if-else block statement
     *    2. Call Add Rule on the if-else tree
     *    3. For each if-else tree:
     *        a. Find the field name from the arguments
     *        b. Create validation key '<validator object>.<validation function name>'
     *        c. Add the rest of the arguments for the validation function (if any)
     *        d. Push object to the fieldGroup
     *        e. Check if the if block body contains another if-else block.
     *            - If yes, call _addRule with operation true (means to treat the
     *                next rule as AND operation)
     *            - If no, check if alternate body contains another if-else block. If yes,
     *                call _addRule with opertaion false (treat next rule as OR operation)
     **/

    var block = jsonTree.body[0].body.body;
    var initialValidation;
    for (var i = 0; i < block.length; i++) {
      if (block[i].type === 'IfStatement') {
        initialValidation = block[i];
        break;
      }
    }

    var fieldGroup = {};
    _addRule(fieldGroup, initialValidation, functionMap, AND);

    return fieldGroup;
  }

  function _findFieldArgument(args) {
    for (var j = 0; j < args.length; j++) {
      if (args[j].type === 'MemberExpression') {
        return args[j];
      }
    }
  }

  function _addRule (fieldGroup, rule, functionMap, operation) {
    // Find validation rule argument
    var field = _findFieldArgument(rule.test.arguments);

    var fieldName = field.property.name;

    // ie. coreValidator.maxLength
    var validationKey = rule.test.callee.object.name + '.' + rule.test.callee.property.name;

    if (!fieldGroup[fieldName]) {
      fieldGroup[fieldName] = [];
    }

    var obj = {
      fieldName: fieldName,
      operation: operation,
      validation: validationKey
    };

    // Adding the rest of validation function arguments (if any)
    var argsValidationRule = functionMap[validationKey].arguments;
    if (argsValidationRule.length > 1) {
      obj.arguments = {};
      angular.forEach(argsValidationRule, function (val, index) {
        if (!val.startsWith('<field:')) {
          var argValue = rule.test.arguments[index];
          obj.arguments[val] = argValue.value;
        }
      });
    }

    fieldGroup[fieldName].push(obj);

    // Recursive call
    if (rule.consequent.body.length && rule.consequent.body[0].type === 'IfStatement') {
      // Treat next rule as AND
      _addRule(fieldGroup, rule.consequent.body[0], functionMap, AND);
    } else if (rule.alternate.body.length && rule.alternate.body[0].type === 'IfStatement') {
      // Treat next rule as OR
      _addRule(fieldGroup, rule.alternate.body[0], functionMap, OR);
    }
  }

  return {
    initValidationFields: initValidationFields
  };
}

angular.module(PKG.name + '.commons')
  .factory('ValidatorFactory', ValidatorFactory);
