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
import { objectQuery, removeEmptyJsonValues } from 'services/helpers';
import If from 'components/If';
import PropertyRow from './PropertyRow';
import { getCurrentNamespace } from 'services/NamespaceStore';
import ThemeWrapper from 'components/ThemeWrapper';
import {
  filterByCondition,
  IFilteredConfigurationGroup,
} from 'components/ConfigurationGroup/utilities/DynamicPluginFilters';
import { IErrorObj } from 'components/ConfigurationGroup/utilities';

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
  errors: {
    [property: string]: IErrorObj[];
  };
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
    [values, pluginProperties]
  );

  // This onUnMount is to make sure we clear out all properties that are hidden.
  React.useEffect(() => {
    return () => {
      const newValues = { ...referenceValueForUnMount.current.values };
      const configGroups = referenceValueForUnMount.current.configurationGroups;
      if (configGroups) {
        configGroups.forEach((group) => {
          group.properties.forEach((property) => {
            if (property.show === false) {
              delete newValues[property.name];
            }
          });
        });
      }
      changeParentHandler(newValues);
    };
  }, []);
  function changeParentHandler(updatedValues) {
    if (!onChange || typeof onChange !== 'function') {
      return;
    }

    onChange(removeEmptyJsonValues(updatedValues));
  }

  const extraConfig = {
    namespace: getCurrentNamespace(),
    properties: values,
    inputSchema,
  };

  // Used to keep track of error messages that found a widget
  // required to identify errors that did not find a widget,
  // they will be marked as orphan errors.
  const [usedErrors, setUsedErrors] = React.useState({});
  const [groups, setGroups] = React.useState([]);
  const [orphanErrors, setOrphanErrors] = React.useState([]);
  React.useEffect(
    () => {
      setGroups(constructGroups);
    },
    [configurationGroups, errors]
  );
  React.useEffect(
    () => {
      setOrphanErrors(constructOrphanErrors);
    },
    [usedErrors]
  );
  function constructGroups() {
    const newUsedErrors = {};
    const newGroups = configurationGroups.map((group, i) => {
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
              // Check if a field is present to display the error contextually
              const errorObjs =
                errors && errors.hasOwnProperty(property.name) ? errors[property.name] : null;
              if (errorObjs) {
                // Mark error as used
                newUsedErrors[property.name] = errors[property.name];
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
                  errors={errorObjs}
                />
              );
            })}
          </div>
        </div>
      );
    });
    setUsedErrors(newUsedErrors);
    return newGroups;
  }

  function constructOrphanErrors() {
    const orphanedErrors = new Set();
    // Error messages that are already displayed in a field should not be
    // displayed in the common area - orphan errors.
    const usedMessages = new Set();
    Object.values(usedErrors).forEach((errs: IErrorObj[]) => {
      errs.forEach((error) => {
        usedMessages.add(error.msg);
      });
    });

    if (errors) {
      Object.keys(errors).forEach((propName) => {
        if (propName === 'orphanErrors') {
          errors.orphanErrors.forEach((orphanError: IErrorObj) => {
            // Making use the error msg has not been displayed contextually
            // elsewhere.
            if (!usedMessages.has(orphanError.msg)) {
              orphanedErrors.add(orphanError.msg);
            }
          });
        }
        // If the error is not used and if the message is not used,
        // add it to orphan errors.
        else if (!usedErrors.hasOwnProperty(propName)) {
          errors[propName].forEach((error: IErrorObj) => {
            if (!usedMessages.has(error.msg)) {
              // If any error is not displayed contextually, and the error message
              // is not used by any other field, mark the error as orphan.
              orphanedErrors.add(error.msg);
            }
          });
        }
      });
    }
    return Array.from(orphanedErrors);
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
      {groups}
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
};
