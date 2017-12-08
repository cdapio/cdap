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
import myExploreApi from 'api/explore';
import {MyDatasetApi} from 'api/dataset';
import ExploreTablesActions from 'services/ExploreTables/ExploreTablesActions';
import {Observable} from 'rxjs/Observable';

const fetchTables = (namespace) => {
  return (dispatch) => {
    let exploreTables$ =  myExploreApi.fetchTables({ namespace });
    let datasetsSpec$ = MyDatasetApi.list({ namespace });
    return Observable
      .combineLatest(
        exploreTables$,
        datasetsSpec$
      )
      .subscribe(res => {
        dispatch({
          type: ExploreTablesActions.SET_TABLES,
          payload: {
            exploreTables: res[0],
            datasetsSpec: res[1]
          }
        });
      });
  };
};

export {fetchTables};
