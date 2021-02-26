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

import {
  generateTableKey,
  constructTablesSelection,
  convertConfigToState,
} from 'components/Replicator/utilities';
import { DML } from 'components/Replicator/types';
import { List, Map, Set } from 'immutable';

type IColumn = Map<string, string>;

describe('Replication Utilities', () => {
  describe('Generate Table Key', () => {
    const DB = 'dbName';
    const TABLE = 'tableName';

    const CORRECT_KEY = `db-${DB}-table-${TABLE}`;

    it('should generate correct key when tableInfo is plain object', () => {
      const tableInfo = {
        database: DB,
        table: TABLE,
      };

      const key = generateTableKey(tableInfo);
      expect(key).toBe(CORRECT_KEY);
    });

    it('should  generate correct key when tableInfo is an Immutable Map', () => {
      const tableInfo = Map({
        database: DB,
        table: TABLE,
      });

      const key = generateTableKey(tableInfo);
      expect(key).toBe(CORRECT_KEY);
    });
  });

  describe('Construct Table Selection', () => {
    let tables: Map<string, Map<string, string>> = Map();
    let columns: Map<string, List<IColumn>> = Map();
    const dmlBlacklist: Map<string, Set<DML>> = Map();

    beforeAll(() => {
      // Generate tables
      const rawTable = [
        {
          database: 'db1',
          table: 'table1',
          schema: 'schema1',
        },
        {
          database: 'db1',
          table: 'table2',
          schema: 'schema1',
        },
        {
          database: 'db1',
          table: 'table3',
          schema: 'schema1',
        },
      ];

      rawTable.forEach((tableInfo) => {
        const key = generateTableKey(tableInfo);
        tables = tables.set(key, Map(tableInfo));

        let rawColumns = List<IColumn>();
        rawColumns = rawColumns.push(
          Map({
            name: `col-${tableInfo.table}-1`,
            type: 'string',
          })
        );
        rawColumns = rawColumns.push(
          Map({
            name: `col-${tableInfo.table}-2`,
            type: 'string',
          })
        );

        columns = columns.set(key, rawColumns);
      });
    });

    it('should return empty array when no table is selected', () => {
      const result = constructTablesSelection(null, null, null);
      expect(result).toEqual([]);
    });

    it('should return empty array when no table is selected but columns exist', () => {
      const result = constructTablesSelection(null, columns, null);
      expect(result).toEqual([]);
    });

    it('should return empty array when no table is selected but dmlEvents exist', () => {
      const result = constructTablesSelection(null, null, dmlBlacklist);
      expect(result).toEqual([]);
    });

    it('should generate table selection', () => {
      const result = constructTablesSelection(tables, columns, dmlBlacklist);

      expect(result).toHaveLength(3);
      expect(result[0].database).toBe('db1');
      expect(result[0].table).toBe('table1');
      expect(result[0].schema).toBe('schema1');

      expect(result[0].columns).toHaveLength(2);
      expect(result[0].columns[0].name).toBe('col-table1-1');
      expect(result[0].columns[1].name).toBe('col-table1-2');
    });
  });

  describe('Convert JSON Config to State', () => {
    let replicationJSON;
    const parentArtifact = {
      name: 'parent-artifact',
      version: 'parent-version',
      scope: 'parent-scope',
    };

    beforeEach(() => {
      replicationJSON = {
        name: 'replication-name',
        artifact: {
          name: 'replication-artifact',
          version: 'artifact-version',
          scope: 'artifact-scope',
        },
        config: {
          description: 'some description',
          connections: [],
          stages: [],
          tables: [
            {
              database: 'db',
              table: 'table1',
              schema: 'schema',
            },
            {
              database: 'db',
              table: 'table2',
              schema: 'schema',
            },
          ],
          offsetBasePath: 'offset-path',
          parallelism: {
            numInstances: 2,
          },
        },
      };
    });

    // TODO: add tests for source and target config
    // need to figure out how to mock API call with toPromise

    it('should generate valid state object', async () => {
      const stateObj = await convertConfigToState(replicationJSON, parentArtifact);

      expect(stateObj.name).toBe('replication-name');
      expect(stateObj.description).toBe('some description');
      expect(stateObj.offsetBasePath).toBe('offset-path');
      expect(stateObj.numInstances).toBe(2);
      expect(stateObj.tables.size).toBe(2);
    });

    it('should generate the correct activeStep', async () => {
      const stateObj = await convertConfigToState(replicationJSON, parentArtifact);

      expect(stateObj.activeStep).toBe(3);
    });
  });
});
