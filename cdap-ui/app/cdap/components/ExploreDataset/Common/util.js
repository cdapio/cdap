/*
 * Copyright © 2017 Cask Data, Inc.
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

import find from 'lodash/find';
import remove from 'lodash/remove';
import isEmpty from 'lodash/isEmpty';
import isNil from 'lodash/isNil';
import cookie from 'react-cookie';
import { USE_REMOTE_SERVER, RAF_ACCESS_TOKEN } from 'components/FeatureUI/config';
import { Theme } from 'services/ThemeHelper';

export function toCamelCase(value) {
  return value.replace(/(\w)(.*?)\b/g, function (result, group1, group2) {
    return result ? (group1.toUpperCase() + (group2 ? group2 : '')) : result;
  });
}

export function getPropertyUpdateObj(property, subPropertyName, schemaName, schemaColumns) {
  let updateObj = {
    property: property.paramName,
    schemaName: schemaName,
    schemaColumns: schemaColumns
  };
  if (isEmpty(property.subParams)) {
    updateObj.subProperty = "none";
    updateObj.isSingleSelect = !property.isCollection;
  } else {
    updateObj.subProperty = subPropertyName;
    let subProperty = find(property.subParams, { paramName: subPropertyName });
    updateObj.isSingleSelect = subProperty && !subProperty.isCollection;
  }
  return updateObj;
}

export function updatePropertyMapWithObj(propertyMap, updateObj) {
  let mappedProperty = propertyMap.get(updateObj.property);
  if (mappedProperty) {
    let mappedPropertyValue = find(mappedProperty, { header: updateObj.subProperty });
    if (mappedPropertyValue) {
      if (!updateObj.isSingleSelect) {
        let schemaValueMap = mappedPropertyValue.value;
        if (isEmpty(updateObj.schemaColumns)) {
          schemaValueMap.delete(updateObj.schemaName);
        } else {
          schemaValueMap.set(updateObj.schemaName, updateObj.schemaColumns);
        }
      } else {
        if (isEmpty(updateObj.schemaColumns)) {
          remove(mappedProperty, { header: updateObj.subProperty });
        } else {
          mappedPropertyValue.value = new Map([[updateObj.schemaName, updateObj.schemaColumns]]);
        }
      }
    } else if (!isEmpty(updateObj.schemaColumns)) {
      mappedProperty.push({
        header: updateObj.subProperty,
        isCollection: !updateObj.isSingleSelect,
        value: new Map([[updateObj.schemaName, updateObj.schemaColumns]])
      });
    }
  } else if (!isEmpty(updateObj.schemaColumns)) {
    propertyMap.set(updateObj.property, [{
      header: updateObj.subProperty,
      isCollection: !updateObj.isSingleSelect,
      value: new Map([[updateObj.schemaName, updateObj.schemaColumns]])
    }]);
  }
}

export function removeSchemaFromPropertyMap(propertyMap, schema) {
  if (!isEmpty(propertyMap)) {
    propertyMap.forEach((value) => {
      if (value) {
        value.forEach(subParam => {
          subParam.value.delete(schema);
        });
      }
    });
  }
}

export function getFeatureObject(props) {
  let featureObject = {
    pipelineRunName: props.featureName
  };
  if (!isEmpty(props.selectedSchemas)) {
    featureObject["dataSchemaNames"] = props.selectedSchemas.map(schema => schema.schemaName);
  }
  if (!isEmpty(props.propertyMap)) {
    props.propertyMap.forEach((value, property) => {
      if (value) {
        featureObject[property] = [];
        let subPropObj = {};
        value.forEach(subParam => {
          if (subParam.header == "none") {
            subParam.value.forEach((columns, schema) => {
              if (!isEmpty(columns)) {
                columns.forEach((column) => {
                  if (subParam.isCollection) {
                    featureObject[property].push({
                      table: schema,
                      column: column.columnName
                    });
                  } else {
                    featureObject[property] = {
                      table: schema,
                      column: column.columnName
                    };
                  }
                });
              }
            });
          } else {
            subParam.value.forEach((columns, schema) => {
              if (!isEmpty(columns)) {
                let subPropValue = subParam.isCollection ? [] : {};
                columns.forEach((column) => {
                  if (subParam.isCollection) {
                    subPropValue.push({
                      table: schema,
                      column: column.columnName
                    });
                  } else {
                    subPropValue = {
                      table: schema,
                      column: column.columnName
                    };
                  }
                });
                subPropObj[subParam.header] = subPropValue;
              }
            });
          }
        });
        if (!isEmpty(subPropObj)) {
          featureObject[property].push(subPropObj);
        }
      }
    });
  }
  if (!isEmpty(props.engineConfigurations)) {
    props.engineConfigurations.forEach((configuration) => {
      if (!isEmpty(configuration.value)) {
        switch (configuration.dataType) {
          case 'int':
            if (configuration.isCollection) {
              let values = configuration.value.split(",");
              featureObject[configuration.name] = values.map(value => parseInt(value));
            } else {
              featureObject[configuration.name] = parseInt(configuration.value);
            }
            break;
          default:
            if (configuration.isCollection) {
              featureObject[configuration.name] = configuration.value.split(",");
            } else {
              featureObject[configuration.name] = configuration.value;
            }
        }
      }
    });
  }
  if (!isNil(props.sinkConfigurations)) {
    for (let property in props.sinkConfigurations) {
      featureObject[property] = props.sinkConfigurations[property];
    }
  }
  return featureObject;
}

export function getURLParam(key) {
  var q = window.location.search.match(new RegExp('[?&]' + key + '=([^&#]*)'));
  return q && q[1];
}


export function getEDAObject(props) {
  let edaObject = {};
  if (!isEmpty(props.engineConfigurations)) {
    props.engineConfigurations.forEach((configuration) => {
      if (!isEmpty(configuration.value)) {
        switch (configuration.dataType) {
          // case 'int':
          //   if (configuration.isCollection) {
          //     let values = configuration.value.split(",");
          //     edaObject[configuration.name] = values.map(value => parseInt(value));
          //   } else {
          //     edaObject[configuration.name] = parseInt(configuration.value);
          //   }
          //   break;
          default:
            if (configuration.isCollection) {
              edaObject[configuration.name] = configuration.value.split(",");
            } else {
              edaObject[configuration.name] = configuration.value;
            }
        }
      }
    });
  }

  if (!isNil(props.operationConfigurations)) {
    for (let operation in props.operationConfigurations) {
      if (!isEmpty(operation.value)) {
        if (operation.isCollection) {
          edaObject[operation.name] = operation.value.split(",");
        } else {
          edaObject[operation.name] = operation.value;
        }
      }
    }
  }

  if (!isEmpty(props.availableOperations)) {
    props.availableOperations.forEach(element => {
      if (!isNil(props.operationConfigurations[element.paramName])) {
        edaObject[element.paramName] = {};
        element.subParams.forEach(subElement => {
          if (!isNil(props.operationConfigurations[element.paramName][subElement.paramName])) {
            if (subElement.isSchemaSpecific && subElement.isCollection) {
              edaObject[element.paramName][subElement.paramName] = (props.operationConfigurations[element.paramName][subElement.paramName]).split(",");
            } else {
              edaObject[element.paramName][subElement.paramName] = props.operationConfigurations[element.paramName][subElement.paramName];
            }
          }
        });
      }
    });
  }

  if (!isNil(props.sinkConfigurations)) {
    for (let property in props.sinkConfigurations) {
      edaObject[property] = props.sinkConfigurations[property];
    }
  }

  if (!isNil(props.extraConfigurations)) {
    for (let property in props.extraConfigurations) {
      edaObject[property] = props.extraConfigurations[property];
    }
  }
  return edaObject;
}

export function getUpdatedConfigurationList(availableConfigurations, configList) {
  return availableConfigurations.map((config) => {
    let configObj = find(configList, { name: config.paramName });
    return {
      name: config.paramName,
      displayName: isEmpty(config.displayName) ? config.paramName : config.displayName,
      value: isEmpty(configObj) ? (isEmpty(config.defaultValue) ? "" : config.defaultValue) : configObj.value,
      dataType: config.dataType,
      isCollection: config.isCollection,
      isMandatory: config.isMandatory,
      description: config.description,
    };
  });

}

export function checkResponseError(result) {
  return (isNil(result) ||
    (result.status && result.status > 200) ||
    (result.statusCode && result.statusCode > 200) ||
    (result.response && result.response.status && result.response.status > 200));
}

export function getErrorMessage(error, defaultMessage) {
  let errorMessage = isEmpty(defaultMessage) ? "Error in retrieving data" : defaultMessage;
  if (!isEmpty(error.message)) {
    errorMessage = errorMessage + "\n" + error.message;
  } else if (error.response && !isEmpty(error.response.message)) {
    errorMessage = errorMessage + "\n" + error.response.message;
  }
  return errorMessage;
}

export function getClassNameForHeaderFooter() {
  let className = '';
  if (Theme !== undefined) {
    if (!Theme.showHeader && !Theme.showFooter) {
      className = 'no-header-footer';
    } else if (!Theme.showHeader) {
      className = 'no-header';
    } else if (!Theme.showFooter) {
      className = 'no-footer';
    }
  }
  return className;
}

export function getAccessToken() {
}

export function getDefaultRequestHeader() {
  if (USE_REMOTE_SERVER) {
    return {
      "AccessToken": `Bearer ${RAF_ACCESS_TOKEN}`,
      "Authorization": `Bearer ${RAF_ACCESS_TOKEN}`
    };
  } else {
    return (isNil(cookie.load('CDAP_Auth_Token'))) ? {} : { "AccessToken": `Bearer ${cookie.load('CDAP_Auth_Token')}` };
  }
}
