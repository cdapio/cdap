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
import 'jsplumb';
import { INode, IConnection } from 'components/DAG/DAGProvider';
import { List, fromJS } from 'immutable';
import uuidV4 from 'uuid/v4';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: 'inherit',
      position: 'absolute',
      width: 'inherit',
      '& circle': {
        fill: 'none',
        stroke: 'none',
      },
    },
  };
};

const DAG_CONTAINER_ID = `dag-${uuidV4()}`;

export interface IEndPointArgs {
  element: HTMLElement | string | null;
  params?: EndpointParams;
  referenceParams?: EndpointParams;
  labelId?: string;
}

export interface IInitNodeProps {
  nodeId: string;
  endPointParams?: IEndPointArgs[];
  makeSourceParams?: any;
  makeTargetParams?: any;
  validConnectionHandler?: IValidationConnectionListener;
  registerTypes?: IRegisterTypesProps;
}

export interface IRegisterTypesProps {
  connections: {
    [anyProp: string]: any;
  };
  endpoints: {
    [anyProp: string]: any;
  };
}

export interface INodeComponentProps extends INode {
  initNode?: (initConfig: IInitNodeProps) => void;
  removeNode?: (nodeId: string) => void;
  addEndpoint?: (
    element: HTMLElement | string | null,
    params?: EndpointParams,
    referenceParams?: EndpointParams,
    label?: string
  ) => void;
  removeEndpoint?: (endpointId: string) => void;
}

interface IDAGRendererProps extends WithStyles<typeof styles> {
  nodes: List<INode>;
  connections: List<IConnection>;
  addConnection: (connection) => void;
  removeConnection: (connection) => void;
  removeNode: (nodeId: string) => void;
  jsPlumbSettings: object;
}

interface IConnectionObjWithDOM {
  source: HTMLElement;
  target: HTMLElement;
}

interface IConnectionObj extends IConnection {
  connection: IConnectionObjWithDOM;
}

type IValidationConnectionListener = (connectionObj: IConnectionObj) => boolean;

class DAGRendererComponent extends React.Component<IDAGRendererProps, any> {
  public state = {
    isJsPlumbInstanceCreated: false,
    jsPlumbInstance: jsPlumb.getInstance(this.props.jsPlumbSettings || {}),
  };

  private validConnectionListeners: IValidationConnectionListener[] = [];

  public componentDidMount() {
    jsPlumb.ready(() => {
      const jsPlumbInstance = jsPlumb.getInstance({
        Container: DAG_CONTAINER_ID,
      });
      jsPlumbInstance.setContainer(document.getElementById(DAG_CONTAINER_ID));
      jsPlumbInstance.bind('connection', (connObj: IConnection, originalEvent: boolean) => {
        if (!originalEvent) {
          return;
        }
        const newConnObj = this.getNewConnectionObj(fromJS(connObj));
        this.props.addConnection(newConnObj);
      });
      jsPlumbInstance.bind('connectionDetached', this.removeConnectionHandler);
      jsPlumbInstance.bind('beforeDrop', this.checkForValidIncomingConnection);
      this.setState({
        isJsPlumbInstanceCreated: true,
        jsPlumbInstance,
      });
    });
  }

  private registerTypes = (registerTypes: IRegisterTypesProps) => {
    const { connections, endpoints } = registerTypes;
    if (Object.keys(connections).length) {
      this.state.jsPlumbInstance.registerConnectionTypes(connections);
    }
    if (Object.keys(endpoints).length) {
      this.state.jsPlumbInstance.registerEndpointTypes(endpoints);
    }
  };

  public addEndpoint = (
    element: HTMLElement | string | null,
    params: EndpointParams = {},
    referenceParams: EndpointParams = {},
    labelId?: string | null
  ) => {
    if (!element) {
      return;
    }
    const jsPlumbEndpoint = this.state.jsPlumbInstance.addEndpoint(
      element,
      params,
      referenceParams
    );
    if (labelId) {
      jsPlumbEndpoint.hideOverlay(labelId);
    }
    this.addListenersForEndpoint(jsPlumbEndpoint, element, labelId);
  };

  public removeEndpoint = (endpointId) => {
    this.state.jsPlumbInstance.unbind('connectionDetached');
    const endpoint = this.state.jsPlumbInstance.getEndpoint(endpointId);

    if (endpoint && endpoint.connections) {
      Object.values(endpoint.connections).forEach((conn: IConnection) => {
        const connectionObj: IConnection = this.getNewConnectionObj(
          fromJS({
            sourceId: conn.sourceId,
            targetId: conn.targetId,
            data: conn.getData(),
          })
        );
        this.props.removeConnection(connectionObj);
      });
    }

    this.state.jsPlumbInstance.deleteEndpoint(endpoint);
    this.state.jsPlumbInstance.bind('connectionDetached', this.removeConnectionHandler);
  };

  public removeConnectionHandler = (connObj: IConnection, originalEvent: boolean) => {
    if (!originalEvent) {
      return;
    }
    const newConnObj = this.getNewConnectionObj(
      fromJS({
        sourceId: connObj.sourceId,
        targetId: connObj.targetId,
        data: connObj.getData(),
      })
    );
    this.props.removeConnection(newConnObj);
  };

  public addHoverListener = (endpoint, domCircleEl, labelId) => {
    let domElement = domCircleEl;
    if (typeof domElement === 'string') {
      domElement = document.getElementById(domElement);
    }
    if (!domElement.classList.contains('hover')) {
      domElement.classList.add('hover');
    }
    if (labelId) {
      endpoint.showOverlay(labelId);
    }
  };

  public removeHoverListener = (endpoint, domCircleEl, labelId) => {
    let domElement = domCircleEl;
    if (typeof domElement === 'string') {
      domElement = document.getElementById(domElement);
    }
    if (domElement.classList.contains('hover')) {
      domElement.classList.remove('hover');
    }
    if (labelId) {
      endpoint.hideOverlay(labelId);
    }
  };

  // TODO: labelId will be used on nodes with endpoints that have labels (condition, error, alert etc.,)
  public addListenersForEndpoint = (endpoint, domCircleEl, labelId = null) => {
    endpoint.canvas.removeEventListener('mouseover', this.addHoverListener);
    endpoint.canvas.removeEventListener('mouseout', this.removeHoverListener);
    endpoint.canvas.addEventListener(
      'mouseover',
      this.addHoverListener.bind(this, endpoint, domCircleEl, labelId)
    );
    endpoint.canvas.addEventListener(
      'mouseout',
      this.removeHoverListener.bind(this, endpoint, domCircleEl, labelId)
    );
  };

  public makeNodeDraggable = (id: string) => {
    this.state.jsPlumbInstance.draggable(id);
  };

  public makeConnections = () => {
    if (!this.state.jsPlumbInstance) {
      return;
    }
    this.props.connections.forEach((connObj) => {
      const newConnObj = this.getNewConnectionObj(connObj).toJSON();
      if (
        (this.state.jsPlumbInstance.getEndpoints(newConnObj.sourceId).length ||
          this.state.jsPlumbInstance.isSource(newConnObj.sourceId)) &&
        (this.state.jsPlumbInstance.getEndpoints(newConnObj.targetId).length ||
          this.state.jsPlumbInstance.isTarget(newConnObj.targetId))
      ) {
        newConnObj.source = newConnObj.sourceId;
        newConnObj.target = newConnObj.targetId;
        this.state.jsPlumbInstance.connect(newConnObj);
      }
    });
  };

  /**
   * Creates a new connection: IConnection object with sourceId and targetId.
   *
   * i/o: Map(Connection)
   * o/p: Map(Connection)
   */
  public getNewConnectionObj = (connObj: IConnection): IConnection => {
    if (connObj.data) {
      return fromJS({
        data: connObj.get('data') || {},
        sourceId: connObj.get('sourceId'),
        targetId: connObj.get('targetId'),
      });
    }
    return fromJS({
      sourceId: connObj.get('sourceId'),
      targetId: connObj.get('targetId'),
    });
  };

  public initNode = ({
    nodeId,
    endPointParams = [],
    makeSourceParams = {},
    makeTargetParams = {},
    validConnectionHandler,
    registerTypes,
  }: IInitNodeProps) => {
    endPointParams.map((endpoint) => {
      const { element, params, referenceParams, labelId } = endpoint;
      this.addEndpoint(element, params, referenceParams, labelId);
    });
    if (Object.keys(makeSourceParams).length) {
      this.state.jsPlumbInstance.makeSource(nodeId, makeSourceParams);
    }
    if (Object.keys(makeTargetParams).length) {
      this.state.jsPlumbInstance.makeTarget(nodeId, makeTargetParams);
    }
    this.makeNodeDraggable(nodeId);
    if (validConnectionHandler) {
      this.validConnectionListeners.push(validConnectionHandler);
    }
    if (registerTypes) {
      this.registerTypes(registerTypes);
    }
  };

  public updateNode = ({
    nodeId,
    endPointParams = [],
    makeSourceParams = {},
    makeTargetParams = {},
    validConnectionHandler,
    registerTypes,
  }: IInitNodeProps) => {
    // TBD
  };

  private checkForValidIncomingConnection = (connObj: IConnectionObj) => {
    return this.validConnectionListeners.reduce((prev, curr) => prev && curr(connObj), true);
  };

  private renderChildren() {
    if (!this.state.isJsPlumbInstanceCreated) {
      return '...loading';
    }

    return React.Children.map(this.props.children, (child: React.ReactElement) => {
      if (
        typeof child === 'string' ||
        typeof child === 'number' ||
        child === null ||
        typeof child === 'undefined' ||
        typeof child === 'boolean'
      ) {
        return child;
      }

      // huh.. This is not how it should be.
      return React.cloneElement(child as React.ReactElement, {
        ...child.props,
        id: child.props.id,
        initNode: this.initNode,
        addEndpoint: this.addEndpoint,
        removeEndpoint: this.removeEndpoint,
        key: child.props.id,
        removeNode: this.props.removeNode,
      });
    });
  }

  public render() {
    const { classes } = this.props;
    return (
      <div style={{ position: 'relative' }}>
        <div id={DAG_CONTAINER_ID} className={classes.root}>
          {this.renderChildren()}
        </div>
      </div>
    );
  }
}

const DAGRenderer = withStyles(styles)(DAGRendererComponent);
export { DAGRenderer };
