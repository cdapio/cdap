/*
 * Copyright © 2019 Cask Data, Inc.
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

import { objectQuery } from 'services/helpers';
import {
  IConfigurationGroup,
  PluginProperties,
  IWidgetProperty,
} from 'components/ConfigurationGroup/types';
import xor from 'lodash/xor';
import flatten from 'lodash/flatten';

interface IDefaultValues {
  [key: string]: string;
}
interface IProcessedConfigurationGroups {
  defaultValues: IDefaultValues;
  configurationGroups: IConfigurationGroup[];
}
export interface IErrorObj {
  msg: string;
  element?: string;
}

interface IPropErrors {
  [propName: string]: IErrorObj[];
}

interface IInputSchemaErrors {
  [stage: string]: { [field: string]: string };
}
interface IOutputSchemaErrors {
  [stage: string]: { [field: string]: string };
}
interface IPropElements {
  [propName: string]: Set<string>;
}

/**
 * processConfigurationGroups will process the plugin properties and widget json to order and group the properties
 *
 * @param pluginProperties Properties from backend
 * @param configurationGroups configuration-groups from Widget JSON
 * @param widgetOuputs widget json outputs
 */
export function processConfigurationGroups(
  pluginProperties: PluginProperties,
  configurationGroups: IConfigurationGroup[] = [],
  widgetOuputs: IWidgetProperty[] = []
): IProcessedConfigurationGroups {
  if (!pluginProperties) {
    return {
      defaultValues: {},
      configurationGroups: [],
    };
  }

  // filter out properties that are not listed by pluginProperties
  const filteredConfigurationGroups = (configurationGroups || []).map((group) => {
    return {
      ...group,
      properties: group.properties
        .filter(
          (property) =>
            pluginProperties[property.name] ||
            (property['widget-category'] && property['widget-category'] === 'plugin')
        )
        .map((property) => {
          return {
            ...property,
            'widget-attributes': property['widget-attributes'] || {},
          };
        }),
    };
  });

  const defaultValues = {};

  filteredConfigurationGroups.forEach((group) => {
    group.properties.forEach((property) => {
      const defaultValue = objectQuery(property, 'widget-attributes', 'default');

      if (defaultValue) {
        defaultValues[property.name] = defaultValue;
      }
    });
  });

  const flattenGroupProperties = flatten(
    filteredConfigurationGroups.map((group) => {
      return group.properties.map((property) => property.name).filter((property) => property);
    })
  );

  // Filter out properties defined as output in widget JSON
  const outputPropertiesName = {};
  (widgetOuputs || []).forEach((output) => {
    outputPropertiesName[output.name] = true;
  });
  const pluginPropertiesName = Object.keys(pluginProperties).filter((propertyName) => {
    return !outputPropertiesName[propertyName];
  });

  // add missing properties under Generic group
  const excludedProperties = xor(flattenGroupProperties, pluginPropertiesName);

  if (excludedProperties.length > 0) {
    const genericGroup = {
      label: 'Generic',
      properties: excludedProperties.map((property) => {
        return {
          label: property,
          name: property,
          'widget-type': 'textbox',
          'widget-attributes': {},
        };
      }),
    };

    filteredConfigurationGroups.push(genericGroup);
  }

  return {
    defaultValues,
    configurationGroups: filteredConfigurationGroups,
  };
}

export function isMacro(value) {
  if (!value || !value.length) {
    return false;
  }

  const beginChar = value.indexOf('${') === 0;
  const endingChar = value.charAt(value.length - 1) === '}';

  return beginChar && endingChar;
}

export function constructErrors(failures) {
  /*
  Structure is
  propertyErrors = {
    prop1Name: [{
      msg: 'error msg',
    }],
    prop2Name: [{
      element: 'keyValuePairRow2',
      msg:'error in second row of keyvalue pair field'
    }]
  }
  prop1Name identifies widgets, element is used to identify sub-elements
  like rows in multi row widget etc.
  */

  const propertyErrors: IPropErrors = {};
  /* Structure for inputSchemaErrors and outputSchemaErrors :
  inputSchemaErrors = {
    'default':{
      field1:'error msg',
      field2: 'error msg'
    },
    'stage1':{
      field1:'error msg',
      field2:'error msg'
    }
  }
  */
  const inputSchemaErrors: IInputSchemaErrors = {};
  const outputSchemaErrors: IOutputSchemaErrors = {};
  const elementsWithErrors: IPropElements = {}; // keep track of elements with errors for each property
  const propsWithPropErrors = new Set(); // keep track of properties w/property-level errors

  // helper for adding an error to error object
  const addError = (errorObj, newError, key) => {
    if (errorObj.hasOwnProperty(key)) {
      errorObj[key].push(newError);
    } else {
      errorObj[key] = [newError];
    }
  };

  failures.forEach((failure) => {
    const errorMsg = constructErrorMessage(failure.message, failure.correctiveAction);
    if (failure.causes) {
      failure.causes.forEach((cause) => {
        if (cause.attributes) {
          const { stageConfig, inputField, outputField } = cause.attributes;
          // stageConfig is the field used to identify that the error belongs
          // to plugin properties.
          if (stageConfig) {
            const err: IErrorObj = { msg: errorMsg };
            const { configElement } = cause.attributes;
            // If configElement is present, it represents that the error is
            // in a complex widget i.e second row in a multirow widget.
            if (configElement) {
              err.element = configElement;
              if (!elementsWithErrors.hasOwnProperty(stageConfig)) {
                addError(propertyErrors, err, stageConfig);
                // mark as an element we've seen errors for
                elementsWithErrors[stageConfig] = new Set([configElement]);
              } else if (
                elementsWithErrors.hasOwnProperty(stageConfig) &&
                !elementsWithErrors[stageConfig].has(configElement)
              ) {
                addError(propertyErrors, err, stageConfig);
                elementsWithErrors[stageConfig].add(configElement);
              }
            }
            // For property-level errors, keep only first property-level error
            else if (!propsWithPropErrors.has(stageConfig)) {
              addError(propertyErrors, err, stageConfig);
              propsWithPropErrors.add(stageConfig);
            }
          }
          // inputField is the field used to identify that the error occurs in
          // inputSchema
          else if (inputField) {
            const { inputStage, stage } = cause.attributes;
            const errorStage = inputStage ? inputStage : stage;
            insertIntoObj(inputSchemaErrors, errorStage, inputField, errorMsg);
          }
          // outputField is the field used to identify that the error occurs
          // in outputSchema
          else if (outputField) {
            const { outputPort } = cause.attributes;
            // noSchemaSection is used when there are no ports in op schema
            const errorStage = outputPort ? outputPort : 'noSchemaSection';
            insertIntoObj(outputSchemaErrors, errorStage, outputField, errorMsg);
          } else {
            const err = { msg: errorMsg };
            // Errors with no specific affiliation - orphan errors.
            addError(propertyErrors, err, 'orphanErrors');
          }
        } else {
          // Errors with no specific affiliation - orphan errors.
          const err = { msg: errorMsg };
          addError(propertyErrors, err, 'orphanErrors');
        }
      });
    }
  });
  return { inputSchemaErrors, outputSchemaErrors, propertyErrors };
}

function insertIntoObj(
  obj: IInputSchemaErrors | IOutputSchemaErrors,
  field: string,
  key: string,
  value: string
) {
  /*
   This helper function inserts kvpairs into object in the format:
  {
   field1: {
     key1 : value1,
     key2 : value2
     },
   field2: {
     key1 : value1,
     key2 : value2
     }
  }
  */
  if (obj.hasOwnProperty(field)) {
    obj[field][key] = value;
  } else {
    obj[field] = { [key]: value };
  }
}

function constructErrorMessage(error: string, correctiveAction: string) {
  let message = '';
  if (error) {
    message += error;
  }
  if (correctiveAction) {
    message += ` - ${correctiveAction}`;
  }
  return message;
}
// Calculates unique number of error messages.
export function countErrors(
  propertyErrors: IPropErrors,
  inputSchemaErrors: IInputSchemaErrors,
  outputSchemaErrors: IOutputSchemaErrors
) {
  const messages = new Set();
  const schemaErrors = [inputSchemaErrors, outputSchemaErrors];
  // Property errors
  Object.values(propertyErrors).forEach((errors) => {
    errors.forEach((error) => {
      messages.add(error.msg);
    });
  });
  // For ip and op schema errors
  schemaErrors.forEach((errorSet) => {
    // For every stage
    Object.values(errorSet).forEach((stageErrors) => {
      // For every message
      Object.values(stageErrors).forEach((msg) => {
        messages.add(msg);
      });
    });
  });

  return messages.size;
}
