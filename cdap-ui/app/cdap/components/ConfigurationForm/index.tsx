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
import If from 'components/If';
import flatten from 'lodash/flatten';
import xor from 'lodash/xor';
import { IConfigurationGroup, PluginProperties } from 'components/ConfigurationForm/types';
import AbstractWidget from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';

/**
 * TODO: This file should be refactored. The groups rendering and also individual property can be a separate component.
 */

const styles = (theme): StyleRules => {
  return {
    root: {
      marginBottom: '35px',
    },
    group: {
      marginBottom: '35px',
    },
    row: {
      marginBottom: '15px',
    },
    required: {
      content: '*',
      color: theme.palette.red[100],
      marginLeft: '2px',
    },
  };
};

interface IConfigurationFormProps extends WithStyles<typeof styles> {
  pluginProperties?: PluginProperties;
  widgetJson?: {
    'configuration-groups'?: IConfigurationGroup[];
  };
  values: Record<string, string>;
  onChange: (value: Record<string, string>) => void;
}

function processConfigurationGroups(
  pluginProperties: PluginProperties,
  configurationGroups: IConfigurationGroup[] = []
) {
  // filter out properties that are not listed by pluginProperties
  const filteredConfigurationGroups = configurationGroups.map((group) => {
    return {
      ...group,
      properties: group.properties
        .filter((property) => pluginProperties[property.name])
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
      return group.properties.map((property) => property.name);
    })
  );

  const pluginPropertiesName = Object.keys(pluginProperties);
  const excludedProperties = xor(flattenGroupProperties, pluginPropertiesName);

  // add missing properties under Generic group
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

class ConfigurationFormView extends React.PureComponent<IConfigurationFormProps> {
  public componentDidMount() {
    // create 'Generic' group for leftover fields
    const { defaultValues, configurationGroups } = processConfigurationGroups(
      this.props.pluginProperties,
      this.props.widgetJson['configuration-groups']
    );

    this.values = {
      ...defaultValues,
      ...this.props.values,
    };

    this.setState({ configurationGroups });
  }

  public state = {
    configurationGroups: [],
    values: '',
  };

  private values = {};

  private handleChange = (propertyName, value) => {
    this.values[propertyName] = value;

    if (this.props.onChange && typeof this.props.onChange === 'function') {
      this.props.onChange({ ...this.values });
    }
  };

  public render() {
    const { classes } = this.props;

    return (
      <div className={classes.root}>
        <pre>{this.state.values}</pre>
        {this.state.configurationGroups.map((group, i) => {
          return (
            <div key={`${group.label}${i}`} className={classes.group}>
              <h2>{group.label}</h2>
              <If condition={group.description && group.description.length > 0}>
                <div>
                  <em>{group.description}</em>
                </div>
              </If>

              <div>
                {group.properties.map((property) => {
                  const widgetType = property['widget-type'];
                  if (widgetType === 'hidden') {
                    return null;
                  }

                  return (
                    <div key={property.name} className={classes.row}>
                      <div>
                        <strong title={this.props.pluginProperties[property.name].description}>
                          {property.label}
                          <If condition={this.props.pluginProperties[property.name].required}>
                            <span className={classes.required}>*</span>
                          </If>
                        </strong>
                      </div>
                      <AbstractWidget
                        type={widgetType}
                        value={this.values[property.name] || ''}
                        onChange={this.handleChange.bind(null, property.name)}
                        widgetProps={property['widget-attributes']}
                        extraConfig={{}}
                      />
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    );
  }
}

const ConfigurationForm = withStyles(styles)(ConfigurationFormView);
export default ConfigurationForm;
