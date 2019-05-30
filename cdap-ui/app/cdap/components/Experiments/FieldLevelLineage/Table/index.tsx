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
