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

interface ITable {
  name: string;
  numColumns: number;
  schemaIssues: number;
  partialSupport: number;
  notSupported: number;
}

interface IAssessmentTableProps {
  tables: ITable[];
  onTableClick: (table: string) => void;
}

const AssessmentTableView: React.FC<IAssessmentTableProps> = ({ tables, onTableClick }) => {
  return (
    <div>
      <table className="table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Number of columns</th>
            <th>Schema issues detected</th>
            <th>Partially supported</th>
            <th>Not supported</th>
            <th />
          </tr>
        </thead>

        <tbody>
          {tables.map((row) => {
            const label =
              row.schemaIssues + row.partialSupport + row.notSupported === 0
                ? 'Mappings'
                : 'Preview';
            return (
              <tr key={row.name}>
                <td>{row.name}</td>
                <td>{row.numColumns}</td>
                <td>{row.schemaIssues}</td>
                <td>{row.partialSupport}</td>
                <td>{row.notSupported}</td>
                <td>
                  <span onClick={onTableClick.bind(null, row.name)}>{label}</span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default AssessmentTableView;
