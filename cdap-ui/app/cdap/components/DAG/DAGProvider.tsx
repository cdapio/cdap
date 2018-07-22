/*
 * Copyright © 2018 Cask Data, Inc.
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
import {List, Map} from 'immutable';

export interface INode {
  id: string;
  name: string;
  properties: object;
}
export interface IConnection {
  from: string;
  to: string;
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
}

export const MyContext = React.createContext({} as IDagStore);

export class DAGProvider extends React.Component<any, IDagProviderState> {

  private getNodes = (): List<INode> => this.state.nodes;
  private addNode = (node: INode) => {
    const newNodes = this.state.nodes.push(node);
    this.setState({
      nodes: newNodes,
    });
  }

  public readonly state = {
    adjancecyMap: Map({}),
    connections: List([]),
    nodes: List([]),
  };

  public render() {
    const store: IDagStore = {
      ...this.state,
      addNode: this.addNode,
      getNodes: this.getNodes,
    };
    return (
      <MyContext.Provider value={store}>
        {this.props.children}
      </MyContext.Provider>
    );
  }
}
