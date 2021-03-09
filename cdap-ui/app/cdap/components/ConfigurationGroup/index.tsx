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
import { processConfigurationGroups, removeFilteredProperties } from './utilities';
import { objectQuery } from 'services/helpers';
import defaults from 'lodash/defaults';
import If from 'components/If';
import PropertyRow from './PropertyRow';
import { getCurrentNamespace } from 'services/NamespaceStore';
import ThemeWrapper from 'components/ThemeWrapper';
import {
  filterByCondition,
  IFilteredConfigurationGroup,
} from 'components/ConfigurationGroup/utilities/DynamicPluginFilters';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';
import { h2Styles } from 'components/Markdown/MarkdownHeading';

const styles = (theme): StyleRules => {
  return {
    group: {
      marginBottom: '20px',
    },
    groupTitle: {
      marginBottom: '15px',
      marginLeft: '10px',
      marginRight: '15px',
    },
    h2Title: {
      ...h2Styles(theme).root,
      marginBottom: '5px',
    },
    groupSubTitle: {
      color: theme.palette.grey[200],
    },
  };
};

export interface IConfigurationGroupProps extends WithStyles<typeof styles> {
  widgetJson?: IWidgetJson;
  pluginProperties: PluginProperties;
  values: Record<string, string>;
  inputSchema?: any;
  disabled?: boolean;
  onChange?: (values: Record<string, string>) => void;
  errors: {
    [property: string]: IErrorObj[];
  };
  validateProperties?: () => void;
}

const ConfigurationGroupView: React.FC<IConfigurationGroupProps> = ({
  widgetJson,
  pluginProperties,
  values,
  inputSchema,
  onChange,
  disabled,
  classes,
  errors,
  validateProperties,
}) => {
  const [configurationGroups, setConfigurationGroups] = React.useState([]);
  const referenceValueForUnMount = React.useRef<{
    configurationGroups?: IFilteredConfigurationGroup[];
    values?: Record<string, string>;
  }>({});
  const [filteredConfigurationGroups, setFilteredConfigurationGroups] = React.useState([]);
  const [orphanErrors, setOrphanErrors] = React.useState([]);

  // Initialize the configurationGroups based on widgetJson and pluginProperties obtained from backend
  React.useEffect(() => {
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

    // We don't need to add default values for plugins in published pipeline
    // as they should already have all the properties they are configured with.
    let initialValues = values;
    if (!disabled) {
      // Only add default values for plugin properties that are not already configured
      // by user.
      initialValues = defaults(values, processedConfigurationGroup.defaultValues);
      changeParentHandler(initialValues);
    }

    updateFilteredConfigurationGroup(
      processedConfigurationGroup.configurationGroups,
      initialValues
    );
  }, [widgetJson, pluginProperties]);

  function updateFilteredConfigurationGroup(configGroup, newValues) {
    let newFilteredConfigurationGroup;

    try {
      newFilteredConfigurationGroup = filterByCondition(
        configGroup,
        widgetJson,
        pluginProperties,
        newValues
      );
    } catch (e) {
      newFilteredConfigurationGroup = configGroup;
      // tslint:disable:no-console
      console.log('Issue with applying filters: ', e);
    }

    referenceValueForUnMount.current = {
      configurationGroups: newFilteredConfigurationGroup,
      values: newValues,
    };

    setFilteredConfigurationGroups(newFilteredConfigurationGroup);
    getOrphanedErrors();
  }

  // Watch for changes in values to determine dynamic widget
  React.useEffect(() => {
    if (!configurationGroups || configurationGroups.length === 0) {
      return;
    }
    updateFilteredConfigurationGroup(configurationGroups, values);
  }, [values]);

  // This onUnMount is to make sure we clear out all properties that are hidden.
  React.useEffect(() => {
    return () => {
      const newValues = referenceValueForUnMount.current.values;
      const configGroups = referenceValueForUnMount.current.configurationGroups;
      const updatedFilteredValues = removeFilteredProperties(newValues, configGroups);
      changeParentHandler(updatedFilteredValues);
    };
  }, []);

  React.useEffect(getOrphanedErrors, [errors]);

  function changeParentHandler(updatedValues) {
    if (!onChange || typeof onChange !== 'function') {
      return;
    }

    onChange(updatedValues);
  }

  const extraConfig = {
    namespace: getCurrentNamespace(),
    properties: values,
    inputSchema,
    validateProperties,
  };

  function getPropertyError(propertyName) {
    return errors && errors.hasOwnProperty(propertyName) ? errors[propertyName] : null;
  }

  function getOrphanedErrors() {
    if (!errors) {
      return;
    }

    const orphanedErrors = new Set();
    const shownErrors = new Set();

    filteredConfigurationGroups.forEach((group) => {
      if (group.show === false) {
        return;
      }
      group.properties.forEach((property) => {
        if (property.show === false) {
          return;
        }

        const propertyError = getPropertyError(property.name);
        if (propertyError) {
          propertyError.forEach((error) => {
            shownErrors.add(error.msg);
          });
        }
      });
    });

    Object.values(errors).forEach((errorArr) => {
      errorArr.forEach((error) => {
        if (!shownErrors.has(error.msg)) {
          orphanedErrors.add(error.msg);
        }
      });
    });

    setOrphanErrors(Array.from(orphanedErrors));
  }

  return (
    <div data-cy="configuration-group">
      <If condition={orphanErrors.length > 0}>
        <div>
          <h2>Errors</h2>
          <div className="text-danger">
            {orphanErrors.map((error: string) => (
              <li>{error}</li>
            ))}
          </div>
        </div>
      </If>
      {filteredConfigurationGroups.map((group, i) => {
        if (group.show === false) {
          return null;
        }

        return (
          <div key={`${group.label}-${i}`} className={classes.group}>
            <div className={classes.groupTitle}>
              <h2 className={classes.h2Title}>{group.label}</h2>
              <If condition={group.description && group.description.length > 0}>
                <small className={classes.groupSubTitle}>{group.description}</small>
              </If>
            </div>

            <div>
              {group.properties.map((property, j) => {
                if (property.show === false) {
                  return null;
                }
                // Hiding all plugin functions if pipeline is deployed
                if (
                  disabled &&
                  property.hasOwnProperty('widget-category') &&
                  property['widget-category'] === 'plugin'
                ) {
                  return null;
                }

                const errorObjs = getPropertyError(property.name);

                return (
                  <PropertyRow
                    key={`${property.name}-${j}`}
                    widgetProperty={property}
                    pluginProperty={pluginProperties[property.name]}
                    value={values[property.name]}
                    onChange={changeParentHandler}
                    extraConfig={extraConfig}
                    disabled={disabled}
                    errors={errorObjs}
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
  errors: PropTypes.object,
  validateProperties: PropTypes.func,
};
