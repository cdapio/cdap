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
import { List, Map } from 'immutable';
import * as React from 'react';

export const CreateContext = React.createContext({});

interface ICreateState {
  activeStep: number;
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
  configurationGroups: List<string>;
  groupToInfo: Map<string, Map<string, string>>;
  groupToWidgets: Map<string, List<string>>;
  widgetInfo: Map<string, Map<string, string>>;
  widgetToAttributes: Map<string, Map<string, string>>;
  liveView: boolean;
  outputName: string;
  JSONStatus: JSONStatusMessage;
  filters: List<string>;
  filterToName: Map<string, string>;
  filterToCondition: Map<string, Map<string, string>>;
  filterToShowList: Map<string, List<string>>;
  showToInfo: Map<string, Map<string, string>>;

  setActiveStep: (step: number) => void;
  setBasicPluginInfo: (basicPluginInfo: IBasicPluginInfo) => void;
  setConfigurationGroups: (groups: List<string>) => void;
  setGroupToInfo: (groupToInfo: Map<string, Map<string, string>>) => void;
  setGroupToWidgets: (groupToWidgets: Map<string, List<string>>) => void;
  setWidgetInfo: (widgetInfo: Map<string, Map<string, string>>) => void;
  setWidgetToAttributes: (widgetToAttributes: Map<string, Map<string, string>>) => void;
  setLiveView: (liveView: boolean) => void;
  setOutputName: (outputName: string) => void;
  setPluginState: (pluginState: any) => void;
  setJSONStatus: (JSONStatus: JSONStatusMessage) => void;
  setFilters: (filters: List<string>) => void;
  setFilterToName: (filterToName: Map<string, string>) => void;
  setFilterToCondition: (filterToCondition: Map<string, Map<string, string>>) => void;
  setFilterToShowList: (filterToShowList: Map<string, List<string>>) => void;
  setShowToInfo: (showToInfo: Map<string, Map<string, string>>) => void;
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
