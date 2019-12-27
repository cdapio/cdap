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
import { DefaultNode } from 'components/DAG/Nodes/Default';
import { List, fromJS } from 'immutable';
import uuidV4 from 'uuid/v4';

const DAG_CONTAINER_ID = `dag-${uuidV4()}`;

export interface IEndPointArgs {
  element: HTMLElement | null;
  params?: EndpointParams;
  referenceParams?: EndpointParams;
}

export interface IInitNodeProps {
  nodeId: string;
  endPointParams?: IEndPointArgs[];
  makeSourceParams?: any;
  makeTargetParams?: any;
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
  onDelete?: (nodeId: string) => void;
}

interface IDAGRendererProps {
  nodes: List<INode>;
  connections: List<IConnection>;
  onConnection: (connection) => void;
  onConnectionDetached: (connection) => void;
  onDeleteNode: (nodeId: string) => void;
  jsPlumbSettings: object;
  registerTypes?: IRegisterTypesProps;
}

export class DAGRenderer extends React.Component<IDAGRendererProps, any> {
  public state = {
    isJsPlumbInstanceCreated: false,
    jsPlumbInstance: jsPlumb.getInstance(this.props.jsPlumbSettings || {}),
  };

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
        this.props.onConnection(newConnObj);
      });
      jsPlumbInstance.bind('connectionDetached', (connObj: IConnection, originalEvent: boolean) => {
        if (!originalEvent) {
          return;
        }
        const newConnObj = this.getNewConnectionObj(fromJS(connObj));
        this.props.onConnectionDetached(newConnObj);
      });
      this.registerTypes(jsPlumbInstance);
      this.setState({
        isJsPlumbInstanceCreated: true,
        jsPlumbInstance,
      });
    });
  }

  private registerTypes = (jsPlumbInstance: jsPlumbInstance) => {
    if (typeof this.props.registerTypes === 'undefined') {
      return;
    }
    const { connections, endpoints } = this.props.registerTypes;
    if (Object.keys(connections).length) {
      jsPlumbInstance.registerConnectionTypes(connections);
    }
    if (Object.keys(endpoints).length) {
      jsPlumbInstance.registerEndpointTypes(endpoints);
    }
  };

  public addEndpoint = (
    element: HTMLElement | null,
    params: EndpointParams = {},
    referenceParams: EndpointParams = {}
  ) => {
    if (!element) {
      return;
    }
    this.state.jsPlumbInstance.addEndpoint(element, params, referenceParams);
  };

  public makeNodeDraggable = (id: string) => {
    this.state.jsPlumbInstance.draggable(id);
  };

  public makeConnections = () => {
    if (!this.state.jsPlumbInstance) {
      return;
    }
    this.state.jsPlumbInstance.deleteEveryConnection();
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
  }: IInitNodeProps) => {
    endPointParams.map((endpoint) => {
      const { element, params, referenceParams } = endpoint;
      this.addEndpoint(element, params, referenceParams);
    });
    if (Object.keys(makeSourceParams).length) {
      this.state.jsPlumbInstance.makeSource(nodeId, makeSourceParams);
    }
    if (Object.keys(makeTargetParams).length) {
      this.state.jsPlumbInstance.makeTarget(nodeId, makeTargetParams);
    }
    this.makeNodeDraggable(nodeId);
    this.makeConnections();
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
      return React.cloneElement(child as React.ReactElement<DefaultNode>, {
        ...child.props,
        id: child.props.id,
        initNode: this.initNode,
        key: child.props.id,
        onDelete: this.props.onDeleteNode,
      });
    });
  }

  public render() {
    return (
      <div style={{ position: 'relative' }}>
        <div
          id={DAG_CONTAINER_ID}
          style={{
            height: 'inherit',
            position: 'absolute',
            width: 'inherit',
          }}
        >
          {this.renderChildren()}
        </div>
      </div>
    );
  }
}
