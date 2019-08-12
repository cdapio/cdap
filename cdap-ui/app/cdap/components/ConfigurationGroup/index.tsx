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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { IWidgetJson, PluginProperties } from './types';
import { processConfigurationGroups } from './utilities';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import PropertyRow from './PropertyRow';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (): StyleRules => {
  return {
    group: {
      marginBottom: '35px',
    },
  };
};

interface IConfigurationGroupProps extends WithStyles<typeof styles> {
  widgetJson?: IWidgetJson;
  pluginProperties: PluginProperties;
  values: Record<string, string>;
  inputSchema?: any;
  onChange: (values: Record<string, string>) => void;
}

const ConfigurationGroupView: React.FC<IConfigurationGroupProps> = ({
  widgetJson,
  pluginProperties,
  values,
  inputSchema,
  onChange,
  classes,
}) => {
  const [configurationGroups, setConfigurationGroups] = React.useState([]);

  React.useEffect(
    () => {
      if (!pluginProperties) {
        return;
      }

      const widgetConfigurationGroup = objectQuery(widgetJson, 'configuration-groups');
      const widgetOutputs = objectQuery(widgetJson, 'outputs');
      const processedConfigurationGroup = processConfigurationGroups(
        pluginProperties,
        widgetConfigurationGroup,
        widgetOutputs
      );

      setConfigurationGroups(processedConfigurationGroup.configurationGroups);

      // set default values
      const defaultValues = processedConfigurationGroup.defaultValues;
      const newValues = {
        ...defaultValues,
        ...values,
      };

      onChange(newValues);
    },
    [widgetJson, pluginProperties]
  );

  function handleChange(property) {
    return (value) => {
      const newValues = {
        ...values,
        [property]: value,
      };

      // remove empty string values
      Object.keys(newValues).forEach((propertyName) => {
        if (typeof newValues[propertyName] === 'string' && newValues[propertyName].length === 0) {
          delete newValues[propertyName];
        }
      });

      onChange(newValues);
    };
  }

  const extraConfig = {
    namespace: getCurrentNamespace(),
    properties: values,
    inputSchema,
  };

  return (
    <div>
      {configurationGroups.map((group, i) => {
        return (
          <div key={`${group.label}-${i}`} className={classes.group}>
            <div>
              <h2>{group.label}</h2>
              <If condition={group.description && group.description.length > 0}>
                <small>{group.description}</small>
              </If>
            </div>

            <div>
              {group.properties.map((property) => {
                return (
                  <PropertyRow
                    key={property.name}
                    widgetProperty={property}
                    pluginProperty={pluginProperties[property.name]}
                    value={values[property.name]}
                    onChange={handleChange(property.name)}
                    extraConfig={extraConfig}
                  />
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
};

const ConfigurationGroup = withStyles(styles)(ConfigurationGroupView);
export default ConfigurationGroup;
