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

import React from 'react';
import './Table.css';

interface INode {
  id: string;
  name: string;
  group: number;
}

interface ITableProps {
  nodes: INode[];
  tableName: string;
}

// TO DO: We can probably replace this component with the SortableStickyGrid - will investigate!

export default function Table({ nodes, tableName }: ITableProps) {
  return (
    <div className="table" id={`table-${tableName}`}>
      <div className="table-header">
        {tableName}
        <div className="table-subheader">{`${nodes.length} fields`}</div>
      </div>
      {nodes.map((node) => {
        return (
          <div className="row" id={node.id} key={node.id}>
            {node.name}
          </div>
        );
      })}
    </div>
  );
}
