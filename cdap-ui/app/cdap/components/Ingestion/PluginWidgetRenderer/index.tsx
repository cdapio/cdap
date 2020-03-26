/*
 * Copyright © 2020 Cask Data, Inc.
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
import ConfigurationGroup from 'components/ConfigurationGroup';
import If from 'components/If';
import { Card, TextField } from '@material-ui/core';
import { objectQuery } from 'services/helpers';
import { getIcon, getPluginDisplayName } from 'components/Ingestion/helpers';

const styles = (): StyleRules => {
  return {
    root: {
      height: 'auto',
      padding: '20px',
    },
    labelContainer: {
      padding: '10px',
      paddingLeft: '0px',
      width: '100%',
    },
    heading: {
      display: 'flex',
      justifyContent: 'space-between',
    },
    pluginIconContainer: {
      display: 'flex',
      alignSelf: 'right',
    },
    pluginFAIcon: {
      fontSize: '32px',
    },
    headingTitle: {
      display: 'flex',
      flexDirection: 'column',
    },
    pluginIcon: { width: '64px' },
  };
};

interface IPluginWidgetRendererProps extends WithStyles<typeof styles> {
  configurationGroupProps: any;
  title: string;
  plugin: any;
}

class PluginWidgetRendererView extends React.Component<IPluginWidgetRendererProps> {
  public render() {
    const { classes, configurationGroupProps, title, plugin } = this.props;
    const pluginLabel = getPluginDisplayName(plugin);
    const iconData = objectQuery(plugin, 'widgetJson', 'icon', 'arguments', 'data');
    return (
      <Card className={classes.root}>
        <div className={classes.heading}>
          <div className={classes.headingTitle}>
            <h5>{title.toUpperCase()}</h5>
            <h2>{pluginLabel}</h2>
          </div>
          <div className={classes.pluginIconContainer}>
            <If condition={iconData}>
              <img className={classes.pluginIcon} src={iconData} />
            </If>
            <If condition={!iconData}>
              <div
                className={`${classes.pluginFAIcon} fa ${getIcon(
                  this.props.plugin.name.toLowerCase()
                )}`}
              />
            </If>
          </div>
        </div>
        <div className={classes.labelContainer}>
          <TextField
            label="Label"
            variant="outlined"
            margin="dense"
            fullWidth={true}
            value={configurationGroupProps.label}
            onChange={(event) => configurationGroupProps.onLabelChange(event.target.value)}
          />
        </div>
        <If condition={configurationGroupProps.pluginProperties}>
          <ConfigurationGroup
            pluginProperties={configurationGroupProps.pluginProperties}
            widgetJson={plugin.widgetJson}
            values={configurationGroupProps.values}
            onChange={configurationGroupProps.onChange}
            validateProperties={(cb) => {
              cb();
            }}
          />
        </If>
      </Card>
    );
  }
}

const PluginWidgetRenderer = withStyles(styles)(PluginWidgetRendererView);
export default PluginWidgetRenderer;
