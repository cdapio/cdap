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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Content from 'components/PluginJSONCreator/Create/Content';
import WizardGuideline from 'components/PluginJSONCreator/Create/WizardGuideline';
import {
  CreateContext,
  IBasicPluginInfo,
  ICreateContext,
} from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

export const LEFT_PANEL_WIDTH = 250;

const styles = (): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    content: {
      height: 'calc(100% - 50px)',
      display: 'grid',
      gridTemplateColumns: `${LEFT_PANEL_WIDTH}px 1fr`,

      '& > div': {
        overflowY: 'auto',
      },
    },
  };
};

class CreateView extends React.PureComponent<ICreateContext & WithStyles<typeof styles>> {
  public setActiveStep = (activeStep: number) => {
    this.setState({ activeStep });
  };

  public setBasicPluginInfo = (basicPluginInfo: IBasicPluginInfo) => {
    const { pluginName, pluginType, displayName, emitAlerts, emitErrors } = basicPluginInfo;
    this.setState({
      pluginName,
      pluginType,
      displayName,
      emitAlerts,
      emitErrors,
    });
  };

  public setConfigurationGroups = (configurationGroups: string[]) => {
    this.setState({ configurationGroups });
  };

  public setGroupToInfo = (groupToInfo: any) => {
    this.setState({ groupToInfo });
  };

  public setGroupToWidgets = (groupToWidgets: any) => {
    this.setState({ groupToWidgets });
  };

  public setWidgetToInfo = (widgetToInfo: any) => {
    this.setState({ widgetToInfo });
  };

  public setJsonView = (jsonView: boolean) => {
    this.setState({ jsonView });
  };

  public state = {
    activeStep: 0,
    pluginName: '',
    pluginType: '',
    displayName: '',
    emitAlerts: null,
    emitErrors: null,
    configurationGroups: [],
    groupToInfo: {},
    groupToWidgets: {},
    widgetToInfo: {},
    jsonView: true,

    setActiveStep: this.setActiveStep,
    setBasicPluginInfo: this.setBasicPluginInfo,
    setConfigurationGroups: this.setConfigurationGroups,
    setGroupToInfo: this.setGroupToInfo,
    setGroupToWidgets: this.setGroupToWidgets,
    setWidgetToInfo: this.setWidgetToInfo,
    setJsonView: this.setJsonView,
  };

  public render() {
    return (
      <CreateContext.Provider value={this.state}>
        <div className={this.props.classes.root}>
          <div className={this.props.classes.content}>
            <WizardGuideline />
            <Content />
          </div>
        </div>
      </CreateContext.Provider>
    );
  }
}

const Create = withStyles(styles)(CreateView);
export default Create;
