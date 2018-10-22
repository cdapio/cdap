/*
 * Copyright © 2016 Cask Data, Inc.
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

import thunk from 'redux-thunk';
import { combineReducers, createStore, applyMiddleware, compose } from 'redux';
import ExploreTablesActions from 'services/ExploreTables/ExploreTablesActions';
import uuidV4 from 'uuid/v4';
import findIndex from 'lodash/findIndex';

const defaultAction = {
  type: '',
  payload: {},
  uniqueId: uuidV4(),
};

const defaultInitialState = {
  tables: [],
};

/*
  TL;DR -
  conditions to determine if a dataset is explorable =
    dataset spec properties (explore.table.name & explore.database.name)
  +
    /explore/tables

  - UI will query dataset service endpoint .../<namespace>/data/datasets. This returns all dataset specs
  - UI will query the explore endpoint .../<namespace>/explore/tables. This returns a TableNameInfo all tables in the default database for that namespace (but not the ones that have a custom database)
    - for each dataset:
      - the explore table name is either given as property explore.table.name or defaults to dataset_<name>.
      - if the dataset spec's properties contain explore.database.name, then the table is explorable with that database name and the above table name
      - otherwise, determine whether the dataset is explorable by finding the TableNameInfo for the explore table name.
        - if that entry exists, the table is explorable, and the database name is in the TableNameInfo
        - otherwise the dataset is not explorable
*/

const tables = (state = defaultInitialState.tables, action = defaultAction) => {
  switch (action.type) {
    case ExploreTablesActions.SET_TABLES: {
      let datasetsSpec = action.payload.datasetsSpec
        .filter(
          (dSpec) =>
            dSpec.properties['explore.database.name'] || dSpec.properties['explore.table.name']
        )
        .map((dSpec) => {
          return {
            datasetName: dSpec.name,
            database: dSpec.properties['explore.database.name'] || 'default',
            table: dSpec.properties['explore.table.name'] || '',
          };
        });
      let exploreTables = action.payload.exploreTables;
      let tables = exploreTables.map((tb) => {
        let tableIndex = tb.table.indexOf('_');
        let dbIndex = tb.database.indexOf('_');
        let matchingSpec = datasetsSpec.find((dspec) => {
          let isSameTable =
            dspec.table === (tableIndex !== -1 ? tb.table.slice(tableIndex) : tb.table);
          let isSameDB =
            dspec.database === (dbIndex !== -1 ? tb.database.slice(dbIndex) : tb.table);
          return isSameTable || isSameDB;
        });
        if (matchingSpec) {
          let matchingSpecIndex = findIndex(datasetsSpec, matchingSpec);
          datasetsSpec.splice(matchingSpecIndex, 1);
          return {
            table: matchingSpec.table || tb.table,
            database: matchingSpec.database || tb.database,
          };
        }
        return tb;
      });
      if (datasetsSpec.length) {
        tables = [...tables, ...datasetsSpec];
      }
      return [...(tables || [])];
    }
    default:
      return state;
  }
};

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;
const ExploreTablesStore = createStore(
  combineReducers({
    tables,
  }),
  defaultInitialState,
  composeEnhancers(applyMiddleware(thunk))
);

export default ExploreTablesStore;
