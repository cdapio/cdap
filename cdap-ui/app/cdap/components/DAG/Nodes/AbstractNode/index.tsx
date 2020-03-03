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
import { INodeComponentProps } from 'components/DAG/DAGRenderer';
import { WithStyles } from '@material-ui/core/styles/withStyles';
import {
  endpointPaintStyles,
  endpointTargetEndpointParams,
  genericNodeStyles,
} from 'components/DAG/Nodes/utilities';
import ButtonBase from '@material-ui/core/ButtonBase';

enum ENDPOINT {
  REGULAR = 'regular',
  ALERT = 'alert',
  ERROR = 'error',
  CONDITION_YES = 'condition_yes',
  CONDITION_NO = 'condition_no',
  PORT = 'port',
}

const AbstractNodeStyles = genericNodeStyles();

export interface IAbstractNodeProps<S extends typeof AbstractNodeStyles>
  extends WithStyles<S>,
    INodeComponentProps {}

export class AbstractNode<
  P extends IAbstractNodeProps<typeof AbstractNodeStyles>
> extends React.Component<P, any> {
  public regularEndpointRef: HTMLElement | null;

  public type = '';

  public static ENDPOINT = ENDPOINT;
  public componentDidMount() {
    if (this.props.initNode && typeof this.props.initNode === 'function') {
      const initConfig = {
        endPointParams: this.getEndpointParams(),
        makeTargetParams: endpointTargetEndpointParams(`${this.props.id}-DottedEndPoint`),
        nodeId: this.props.id,
        validConnectionHandler: this.checkForValidIncomingConnection,
        registerTypes: this.getRegisterTypes(),
      };
      this.props.initNode(initConfig);
    }
  }
  public shouldComponentUpdate() {
    return false;
  }

  public getRegisterTypes = () => {
    return null;
  };

  public getEndpointParams = (): any => {
    const endPointConfigs = AbstractNode.getEndpointConfig();
    return [
      {
        element: this.regularEndpointRef,
        params: endPointConfigs,
        referenceParams: {},
      },
    ];
  };

  public static getEndpointConfig = (): any => {
    return {
      Anchor: [1, 0.5, 1, 0, 0, 2], // same as Right but moved down 2px
      isSource: true,
      maxConnections: -1, // -1 means unlimited connections
      Endpoint: 'Dot',
      EndpointStyle: { radius: 20 },
      Connector: [
        'Flowchart',
        { stub: [10, 15], alwaysRespectStubs: true, cornerRadius: 20, midpoint: 0.2 },
      ],
      paintStyle: endpointPaintStyles,
    };
  };

  public getAlertEndpointConfig = () => {
    return {
      ...AbstractNode.getEndpointConfig(),
      anchor: [0.5, 1, 0, 1, 1, 0], // same as Bottom but moved right 2px
    };
  };

  public getErrorEndpointConfig = () => {
    return {
      ...AbstractNode.getEndpointConfig(),
      anchor: [0.5, 1, 0, 1, 2, 0], // same as Bottom but moved right 3px
    };
  };

  public checkForValidIncomingConnection = (c) => {
    return true;
  };

  public renderEndpoint = (classes) => (
    <div
      className={`${classes.endpointCircle} ${classes.regularEndpointCircle}`}
      ref={(ref) => (this.regularEndpointRef = ref)}
      data-node-type={this.type}
      data-endpoint-type={ENDPOINT.REGULAR}
    >
      <div className={classes.endpointCaret} />
    </div>
  );

  public renderContent = () => {
    return (
      <span>
        {this.props.config && this.props.config.label ? this.props.config.label : this.props.id}
      </span>
    );
  };

  public render() {
    const { classes } = this.props;
    return (
      <div id={this.props.id} className={classes.root} data-node-type={this.type}>
        {this.renderContent()}
        {this.renderEndpoint(classes)}
      </div>
    );
  }
}
