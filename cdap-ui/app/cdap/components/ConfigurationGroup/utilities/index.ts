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
