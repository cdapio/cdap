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
import { List, Map } from 'immutable';

export interface INodeConfig extends Map<string, any> {
  label?: string;
  styles?: object;
  properties?: object;
  [newProps: string]: any;
}

export interface INode extends Map<string, any> {
  id: string;
  name: string;
  config: INodeConfig;
}
export interface IConnection extends Map<string, any> {
  [extraProps: string]: any;
  source?: any;
  target?: any;
  sourceId: string;
  targetId: string;
}
export interface IAdjacencyMap {
  [key: string]: string;
}

export interface IDagProviderState {
  adjancecyMap: Map<string, string | object>;
  connections: List<IConnection>;
  nodes: List<INode>;
}
export interface IDagStore extends IDagProviderState {
  addNode: (node: INode) => void;
  addConnection: (connection: IConnection) => void;
  getConnections: () => List<IConnection>;
  getNodes: () => List<INode>;
  removeNode: (node: string) => void;
  removeConnection: (connectionObj: IConnection) => void;
}

export const MyContext = React.createContext({} as Readonly<IDagStore>);

export class DAGProvider extends React.Component<any, IDagProviderState> {
  private getNodes = (): List<INode> => this.state.nodes;
  private addNode = (node: INode) => {
    this.setState({
      nodes: this.state.nodes.push(node),
    });
  };
  private removeNode = (nodeId: string) => {
    this.setState({
      nodes: this.state.nodes.filter((node) => node.get('id') !== nodeId) as List<INode>,
    });
  };
  private getConnections = (): List<IConnection> => this.state.connections;
  private addConnection = (connection: IConnection) => {
    if (
      this.state.connections.find(
        (conn) =>
          conn.get('sourceId') === connection.get('sourceId') &&
          conn.get('targetId') === connection.get('targetId')
      )
    ) {
      return;
    }
    this.setState({
      connections: this.state.connections.push(connection),
    });
  };
  private removeConnection = (connectionObj: IConnection) => {
    const newConnections = this.state.connections.filter(
      (connection: IConnection) =>
        connection.get('from') !== connectionObj.get('from') &&
        connection.get('to') !== connectionObj.get('to')
    ) as List<IConnection>;
    this.setState({
      connections: newConnections,
    });
  };

  public readonly state = {
    adjancecyMap: Map<IAdjacencyMap>({}),
    connections: List(Map([]) as IConnection),
    nodes: List(Map([]) as IConnection),
  };

  public render() {
    const store: Readonly<IDagStore> = Object.freeze({
      ...this.state,
      addConnection: this.addConnection,
      addNode: this.addNode,
      getConnections: this.getConnections,
      getNodes: this.getNodes,
      removeConnection: this.removeConnection,
      removeNode: this.removeNode,
    });
    return <MyContext.Provider value={store}>{this.props.children}</MyContext.Provider>;
  }
}
