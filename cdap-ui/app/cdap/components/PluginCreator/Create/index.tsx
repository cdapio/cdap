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
import LeftPanel from 'components/PluginCreator/Create/LeftPanel';
// import TopPanel from 'components/PluginCreator/Create/TopPanel';
import Content from 'components/PluginCreator/Create/Content';
import { Redirect } from 'react-router-dom';
import { objectQuery } from 'services/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
/*import {
  fetchPluginInfo,
  fetchPluginWidget,
  constructTablesSelection,
} from 'components/PluginCreator/utilities';*/
import { PluginType } from 'components/PluginCreator/constants';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import uuidV4 from 'uuid/v4';
// import { MyPluginCreatorApi } from 'api/replicator';
// import { generateTableKey } from 'components/PluginCreator/utilities';
import { List, Map, Set, fromJS } from 'immutable';
import { Observable } from 'rxjs/Observable';
import { string } from 'prop-types';

export const CreateContext = React.createContext({});
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

interface ICreateProps extends WithStyles<typeof styles> {
  match: {
    params: {
      artifactName: string;
      artifactVersion: string;
      artifactScope: string;
      pluginNam: string;
    };
  };
  history;
}

type IPluginConfig = Record<string, string>;

enum DML {
  insert = 'INSERT',
  update = 'UPDATE',
  delete = 'DELETE',
}

interface IColumn {
  name: string;
  type: string;
}

export interface IWidgetData {
  widgetType: string;
  widgetLabel: string;
  widgetName: string;
  widgetCategory: string;
  widgetAttributes: IWidgetAttributes;
}

export interface IWidgetAttributes {
  placeholder: string;
}

interface ICreateState {
  name: string;
  description: string;
  sourcePluginInfo: any;
  sourcePluginWidget: any;
  targetPluginInfo: any;
  targetPluginWidget: any;
  sourceConfig: IPluginConfig;
  targetConfig: IPluginConfig;
  tables: any;
  columns: any;
  dmlBlacklist: Map<string, Set<DML>>;
  offsetBasePath: string;
  numInstances: number;
  parentArtifact: {
    name: string;
    version: string;
    scope: string;
  };
  draftId: string;
  isInvalidSource: boolean;
  loading: boolean;
  activeStep: number;
  configurationGroups: string[];
  groupsToWidgets: Map<string, Map<string, IWidgetData>>;
  setActiveStep: (step: number) => void;
  setNameDescription: (name: string, description?: string) => void;
  setSourcePluginWidget: (sourcePluginWidget) => void;
  setSourceConfig: (sourceConfig: IPluginConfig) => void;
  setTargetPluginInfo: (targetPluginInfo: any) => void;
  setTargetPluginWidget: (targetPluginWidget) => void;
  setTargetConfig: (targetConfig: IPluginConfig) => void;
  setTables: (tables, columns, dmlBlacklist) => void;
  setAdvanced: (offsetBasePath, numInstances) => void;
  getPluginCreatorConfig: () => any;
  addConfigurationGroup: (group: string) => void;
  setConfigurationGroups: (groups: string[]) => void;
  setGroupsToWidgets: (groupsToWidgets) => void;
}

export type ICreateContext = Partial<ICreateState>;

class CreateView extends React.PureComponent<ICreateProps, ICreateContext> {
  /*public setActiveStep = (step: number) => {
    setTimeout(() => {
      this.saveDraft().subscribe(
        () => {
          this.setState({ activeStep: step });
        },
        (err) => {
          // tslint:disable-next-line: no-console
          console.log('Failed to save draft', err);
        }
      );
    }, 100);
  };*/

  public setNameDescription = (name, description) => {
    this.setState({ name, description }, () => {
      this.props.history.push(
        `/ns/${getCurrentNamespace()}/plugincreation/drafts/${this.state.draftId}`
      );
    });
  };

  public setSourcePluginWidget = (sourcePluginWidget) => {
    this.setState({ sourcePluginWidget });
  };

  public setSourceConfig = (sourceConfig) => {
    this.setState({ sourceConfig });
  };

  public setTargetPluginInfo = (targetPluginInfo) => {
    this.setState({ targetPluginInfo });
  };

  public setTargetPluginWidget = (targetPluginWidget) => {
    this.setState({ targetPluginWidget });
  };

  public setTargetConfig = (targetConfig) => {
    this.setState({ targetConfig });
  };

  public setTables = (tables, columns, dmlBlacklist) => {
    this.setState({ tables, columns, dmlBlacklist });
  };

  public setAdvanced = (offsetBasePath, numInstances) => {
    this.setState({ offsetBasePath, numInstances });
  };

  public addConfigurationGroup = (group) => {
    const newGroups = this.state.configurationGroups;
    newGroups.push(group);
    this.setState({ configurationGroups: newGroups });
  };

  public setConfigurationGroups = (configurationGroups) => {
    this.setState({ configurationGroups });
  };

  public setGroupsToWidgets = (groupsToWidgets) => {
    this.setState({ groupsToWidgets });
  };

  public state = {
    name: '',
    description: '',
    sourcePluginInfo: null,
    sourcePluginWidget: null,
    targetPluginInfo: null,
    targetPluginWidget: null,
    sourceConfig: null,
    targetConfig: null,
    tables: Map<string, Map<string, string>>(),
    columns: Map<string, List<IColumn>>(),
    dmlBlacklist: Map<string, Set<DML>>(),
    offsetBasePath: window.CDAP_CONFIG.delta.defaultCheckpointDir || '',
    numInstances: 1,

    parentArtifact: null,
    draftId: null,
    isInvalidSource: false,
    loading: false,

    activeStep: 0,

    configurationGroups: ['Basic', 'Credentials', 'Advanced'],
    groupsToWidgets: Map<string, Map<string, IWidgetData>>(),

    // setActiveStep: this.setActiveStep,
    setNameDescription: this.setNameDescription,
    setSourcePluginWidget: this.setSourcePluginWidget,
    setSourceConfig: this.setSourceConfig,
    setTargetPluginInfo: this.setTargetPluginInfo,
    setTargetPluginWidget: this.setTargetPluginWidget,
    setTargetConfig: this.setTargetConfig,
    setTables: this.setTables,
    setAdvanced: this.setAdvanced,
    addConfigurationGroup: this.addConfigurationGroup,
    setConfigurationGroups: this.setConfigurationGroups,
    setGroupsToWidgets: this.setGroupsToWidgets,
    // getPluginCreatorConfig: this.getPluginCreatorConfig,
  };

  public componentDidMount() {
    this.setGroupsToWidgets(
      Map({
        Basic: {
          '1': {
            widgetType: 'textbox',
            widgetLabel: 'string1',
            widgetName: 'string2',
            widgetCategory: 'string3',
            widgetAttributes: {
              placeholder: 'hi',
            },
          },
        },
        Advanced: {
          '2': {
            widgetType: 'textbox',
            widgetLabel: 'string1',
            widgetName: 'string2',
            widgetCategory: 'string3',
            widgetAttributes: {
              placeholder: 'hi',
            },
          },
        },
      })
    );
  }

  private redirectToListView = () => {
    return <Redirect to={`/ns/${getCurrentNamespace()}/plugincreation`} />;
  };

  public render() {
    if (this.state.isInvalidSource) {
      return this.redirectToListView();
    }

    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    return (
      <CreateContext.Provider value={this.state}>
        <div className={this.props.classes.root}>
          <div className={this.props.classes.content}>
            <LeftPanel />
            <Content />
          </div>
        </div>
      </CreateContext.Provider>
    );
  }
}

export function createContextConnect(Comp) {
  return (extraProps) => {
    return (
      <CreateContext.Consumer>
        {(props) => {
          const finalProps = {
            ...props,
            ...extraProps,
          };

          return <Comp {...finalProps} />;
        }}
      </CreateContext.Consumer>
    );
  };
}

const Create = withStyles(styles)(CreateView);
export default Create;
