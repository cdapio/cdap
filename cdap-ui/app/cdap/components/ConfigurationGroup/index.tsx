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
import PropTypes from 'prop-types';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { IWidgetJson, PluginProperties } from './types';
import { processConfigurationGroups } from './utilities';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import PropertyRow from './PropertyRow';
import { getCurrentNamespace } from 'services/NamespaceStore';
import ThemeWrapper from 'components/ThemeWrapper';
import {
  filterByCondition,
  IFilteredConfigurationGroup,
} from 'components/ConfigurationGroup/utilities/DynamicPluginFilters';

const styles = (): StyleRules => {
  return {
    group: {
      marginBottom: '45px',
    },
    groupTitle: {
      marginBottom: '25px',
    },
  };
};

interface IConfigurationGroupProps extends WithStyles<typeof styles> {
  widgetJson?: IWidgetJson;
  pluginProperties: PluginProperties;
  values: Record<string, string>;
  inputSchema?: any;
  disabled?: boolean;
  onChange?: (values: Record<string, string>) => void;
}

const ConfigurationGroupView: React.FC<IConfigurationGroupProps> = ({
  widgetJson,
  pluginProperties,
  values,
  inputSchema,
  onChange,
  disabled,
  classes,
}) => {
  const [configurationGroups, setConfigurationGroups] = React.useState([]);
  const referenceValueForUnMount = React.useRef<{
    configurationGroups?: IFilteredConfigurationGroup[];
    values?: Record<string, string>;
  }>({});

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

      let filteredConfigurationGroups;

      try {
        filteredConfigurationGroups = filterByCondition(
          processedConfigurationGroup.configurationGroups,
          widgetJson,
          pluginProperties,
          values
        );
      } catch (e) {
        filteredConfigurationGroups = processedConfigurationGroup.configurationGroups;
        // tslint:disable:no-console
        console.log('Issue with applying filters: ', e);
      }

      setConfigurationGroups(filteredConfigurationGroups);

      // set default values
      const defaultValues = processedConfigurationGroup.defaultValues;
      const newValues = {
        ...defaultValues,
        ...values,
      };

      changeParentHandler(newValues);
      referenceValueForUnMount.current = {
        configurationGroups: filteredConfigurationGroups,
        values: newValues,
      };
    },
    [values]
  );

  // This onUnMount is to make sure we clear out all properties that are hidden.
  React.useEffect(() => {
    return () => {
      const newValues = { ...referenceValueForUnMount.current.values };
      const configGroups = referenceValueForUnMount.current.configurationGroups;
      configGroups.forEach((group) => {
        group.properties.forEach((property) => {
          if (property.show === false) {
            delete newValues[property.name];
          }
        });
      });
      changeParentHandler(newValues);
    };
  }, []);
  function changeParentHandler(updatedValues) {
    if (!onChange || typeof onChange !== 'function') {
      return;
    }

    const newValues = { ...updatedValues };
    // remove empty string values
    Object.keys(newValues).forEach((propertyName) => {
      if (typeof newValues[propertyName] === 'string' && newValues[propertyName].length === 0) {
        delete newValues[propertyName];
      }
    });

    onChange(newValues);
  }

  const extraConfig = {
    namespace: getCurrentNamespace(),
    properties: values,
    inputSchema,
  };

  return (
    <div data-cy="configuration-group">
      {configurationGroups.map((group, i) => {
        if (group.show === false) {
          return null;
        }
        return (
          <div key={`${group.label}-${i}`} className={classes.group}>
            <div className={classes.groupTitle}>
              <h2>{group.label}</h2>
              <If condition={group.description && group.description.length > 0}>
                <small>{group.description}</small>
              </If>
            </div>

            <div>
              {group.properties.map((property, j) => {
                if (property.show === false) {
                  return null;
                }
                return (
                  <PropertyRow
                    key={`${property.name}-${j}`}
                    widgetProperty={property}
                    pluginProperty={pluginProperties[property.name]}
                    value={values[property.name]}
                    onChange={changeParentHandler}
                    extraConfig={extraConfig}
                    disabled={disabled}
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

const StyledConfigurationGroup = withStyles(styles)(ConfigurationGroupView);

function ConfigurationGroup(props) {
  return (
    <ThemeWrapper>
      <StyledConfigurationGroup {...props} />
    </ThemeWrapper>
  );
}
export default ConfigurationGroup;

(ConfigurationGroup as any).propTypes = {
  widgetJson: PropTypes.object,
  pluginProperties: PropTypes.object,
  values: PropTypes.object,
  inputSchema: PropTypes.oneOfType([PropTypes.object, PropTypes.array]),
  disabled: PropTypes.bool,
  onChange: PropTypes.func,
};
