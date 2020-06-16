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
import { IPort } from 'components/DAG//Nodes/SplitterNode';

export interface INodeConfig extends Map<string, any> {
  label?: string;
  styles?: object;
  properties?: object;
  showAlert?: boolean;
  showError?: boolean;
  ports?: IPort[];
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
  getNodes: () => List<INode>;
  addNode: (node: INode) => void;
  removeNode: (node: string) => void;
  updateNode: (nodeId: string, config: INodeConfig) => void;
  getConnections: () => List<IConnection>;
  addConnection: (connection: IConnection) => void;
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
    const newNodes = this.state.nodes.delete(
      this.state.nodes.findIndex((node: INode) => node.get('id') === nodeId)
    );
    this.setState({
      nodes: newNodes,
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
    const newConnections = this.state.connections.delete(
      this.state.connections.findIndex(
        (connection: IConnection) =>
          connection.get('from') === connectionObj.get('from') &&
          connection.get('to') === connectionObj.get('to')
      )
    );
    this.setState({
      connections: newConnections,
    });
  };

  private updateNode = (nodeId: string, newConfig: INodeConfig) => {
    const newNodes = this.state.nodes.update(
      this.state.nodes.findIndex((node: INode) => node.get('id') === nodeId),
      (value: INode) => {
        const finalConfig: INode = value.update('config', (config) => {
          return config.merge(newConfig);
        }) as INode;
        return finalConfig;
      }
    );
    this.setState({
      nodes: newNodes,
    });
  };
  public readonly state: IDagProviderState = {
    adjancecyMap: Map<IAdjacencyMap>({}),
    connections: List<IConnection>(Map([])),
    nodes: List<INode>(Map([])),
  };

  public render() {
    const store: Readonly<IDagStore> = Object.freeze({
      ...this.state,
      getNodes: this.getNodes,
      addNode: this.addNode,
      removeNode: this.removeNode,
      updateNode: this.updateNode,
      getConnections: this.getConnections,
      addConnection: this.addConnection,
      removeConnection: this.removeConnection,
    });
    return <MyContext.Provider value={store}>{this.props.children}</MyContext.Provider>;
  }
}
