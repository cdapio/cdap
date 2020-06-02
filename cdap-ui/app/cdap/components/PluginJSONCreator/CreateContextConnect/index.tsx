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

import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import * as React from 'react';

export const CreateContext = React.createContext({});

interface ICreateState {
  activeStep: number;
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
  configurationGroups: string[];
  groupToInfo: any;
  groupToWidgets: any;
  widgetInfo: any;
  widgetToAttributes: any;
  liveView: boolean;
  outputName: string;
  JSONStatus: JSONStatusMessage;
  filters: string[];
  filterToName: any;
  filterToCondition: any;
  filterToShowList: any;
  showToInfo: any;

  setActiveStep: (step: number) => void;
  setBasicPluginInfo: (basicPluginInfo: IBasicPluginInfo) => void;
  setConfigurationGroups: (groups: string[]) => void;
  setGroupToInfo: (groupToInfo: any) => void;
  setGroupToWidgets: (groupToWidgets: any) => void;
  setWidgetInfo: (widgetInfo: any) => void;
  setWidgetToAttributes: (widgetToAttributes: any) => void;
  setLiveView: (liveView: boolean) => void;
  setOutputName: (outputName: string) => void;
  setPluginState: (pluginState: any) => void;
  setJSONStatus: (JSONStatus: JSONStatusMessage) => void;
  setFilters: (filters: string[]) => void;
  setFilterToName: (filterToName: any) => void;
  setFilterToCondition: (filterToCondition: any) => void;
  setFilterToShowList: (filterToShowList: any) => void;
  setShowToInfo: (showToInfo: any) => void;
}

export interface IBasicPluginInfo {
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
}

export interface IConfigurationGroupInfo {
  label: string;
  description?: string;
}

export interface IWidgetInfo {
  name: string;
  label: string;
  widgetType: string;
  widgetCategory?: string;
}

export type ICreateContext = Partial<ICreateState>;

export function createContextConnect(Context, Component) {
  return (extraProps) => {
    return (
      <Context.Consumer>
        {(props) => {
          const finalProps = {
            ...props,
            ...extraProps,
          };

          return <Component {...finalProps} />;
        }}
      </Context.Consumer>
    );
  };
}
