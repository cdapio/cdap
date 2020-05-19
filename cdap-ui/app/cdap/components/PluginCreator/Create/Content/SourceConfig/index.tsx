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
import {
  createContextConnect,
  ICreateContext,
  IWidgetData,
  IWidgetAttributes,
} from 'components/PluginCreator/Create';
import classnames from 'classnames';
import ConfigurationGroup from 'components/ConfigurationGroup';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
// import { fetchPluginWidget } from 'components/PluginCreator/utilities';
import Button from '@material-ui/core/Button';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
      padding: '15px 40px',
    },
    header: {
      fontSize: '18px',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
    },
    option: {
      marginRight: '100px',
      cursor: 'pointer',
    },
    active: {
      fontWeight: 'bold',
      borderBottom: `3px solid ${theme.palette.grey[200]}`,
    },
    content: {
      paddingTop: '25px',
    },
  };
};

const SourceConfigView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  sourcePluginInfo,
  sourcePluginWidget,
  setSourcePluginWidget,
  sourceConfig,
  setSourceConfig,
  configurationGroups,
  setConfigurationGroups,
  addConfigurationGroup,
  groupsToWidgets,
  setGroupsToWidgets,
}) => {
  const [selectedGroup, setselectedGroup] = React.useState(configurationGroups[0]);
  const [values, setValues] = React.useState(sourceConfig);

  // const pluginProperties = objectQuery(sourcePluginInfo, 'properties');

  const mapping: Record<string, IWidgetData[]> = {
    Basic: [
      {
        widgetType: 'string',
        widgetLabel: 'string',
        widgetName: 'string',
        widgetCategory: 'string',
        widgetAttributes: {
          placeholder: 'string',
        } as IWidgetAttributes,
      } as IWidgetData,
      {
        widgetType: 'string',
        widgetLabel: 'string',
        widgetName: 'string',
        widgetCategory: 'string',
        widgetAttributes: {
          placeholder: 'string',
        } as IWidgetAttributes,
      } as IWidgetData,
    ] as IWidgetData[],
    Advanced: [],
  };

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        {Object.keys(mapping).map((group: string) => {
          return (
            <span
              className={classnames(classes.option, { [classes.active]: group === selectedGroup })}
              onClick={() => setselectedGroup(group)}
            >
              {group}
            </span>
          );
        })}

        <Button variant="contained" color="primary" onClick={() => addConfigurationGroup('new')}>
          + NEW CONFIGURATION
        </Button>
      </div>

      {mapping[selectedGroup].map(({ widgetData: IWidgetData }) => {
        return (
          <div className={classes.content}>
            <WidgetWrapper
              pluginProperty={{
                required: true,
              }}
              widgetProperty={{
                'widget-type': widgetData.widgetType,
                label: widgetData.widgetLabel,
                name: widgetData.widgetName,
                'widget-attributes': {
                  placeholder: widgetData.widgetAttributes.placeholder,
                },
              }}
              value={'connectionArguments'}
            />
          </div>
        );
      })}
    </div>
  );
};

const StyledSourceConfig = withStyles(styles)(SourceConfigView);
const SourceConfig = createContextConnect(StyledSourceConfig);
export default SourceConfig;
