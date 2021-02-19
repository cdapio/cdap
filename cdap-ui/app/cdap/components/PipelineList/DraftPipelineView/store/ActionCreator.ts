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

import MyUserStoreApi from 'api/userstore';
import { MyPipelineApi } from 'api/pipeline';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';
import Store, { Actions, SORT_ORDER } from 'components/PipelineList/DraftPipelineView/store';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';
import orderBy from 'lodash/orderBy';
import { Observable } from 'rxjs/Observable';

const DRAFTS_KEY = 'hydratorDrafts';
const PROPERTY = 'property';

function getOldDrafts(oldDrafts) {
  if (oldDrafts && oldDrafts.statusCode) {
    return [];
  }
  const namespace = getCurrentNamespace();
  const oldDraftsObj = objectQuery(oldDrafts, PROPERTY, DRAFTS_KEY, namespace) || {};

  let drafts: IDraft[] = [];

  Object.keys(oldDraftsObj).forEach((id) => {
    drafts.push({ ...oldDraftsObj[id], needsUpgrade: true });
  });

  drafts = orderBy(drafts, [(draft) => draft.name.toLowerCase()], ['asc']);
  return drafts;
}

export function getDrafts() {
  const namespace = getCurrentNamespace();
  Observable.forkJoin<
    any,
    any /** setting as 'any'. We should remove user store reference in the next major release */
  >(
    MyPipelineApi.getDrafts({
      context: namespace,
    }).catch((err) => Observable.of(err)),
    MyUserStoreApi.get().catch((err) => Observable.of(err))
  ).subscribe(([newDrafts, userStore]) => {
    let drafts = getOldDrafts(userStore);
    // We get a `statusCode` when the service down or a 404 error.
    // For 200 we just get the drafts.
    /**
     * TODO (CDAP-17569): We need to properly handle error if either of the APIs fail.
     * We don't do that today as from a user perspective they don't know
     * what an old and a new draft really is.
     */
    if (newDrafts && !newDrafts.statusCode) {
      drafts = newDrafts.concat(drafts);
    }

    Store.dispatch({
      type: Actions.setDrafts,
      payload: {
        list: drafts,
      },
    });
  });
}

export function reset() {
  Store.dispatch({
    type: Actions.reset,
  });
}

export function deleteDraft(draft: IDraft) {
  const draftId = draft.needsUpgrade ? draft.__ui__.draftId : draft.id;
  let deleteObservable$ = null;
  if (!draft.needsUpgrade) {
    deleteObservable$ = MyPipelineApi.deleteDraft({
      context: getCurrentNamespace(),
      draftId,
    });
  } else {
    deleteObservable$ = MyUserStoreApi.get().mergeMap((res) => {
      const currentNamespace = getCurrentNamespace();
      if (!objectQuery(res, 'property', 'hydratorDrafts', currentNamespace, draftId)) {
        return;
      }
      delete res.property.hydratorDrafts[currentNamespace][draftId];
      return MyUserStoreApi.set({}, res.property);
    });
  }
  deleteObservable$.subscribe(getDrafts);
}

export function setSort(columnName: string) {
  const state = Store.getState().drafts;
  const currentColumn = state.sortColumn;
  const currentSortOrder = state.sortOrder;

  let sortOrder = SORT_ORDER.asc;
  if (currentColumn === columnName && currentSortOrder === SORT_ORDER.asc) {
    sortOrder = SORT_ORDER.desc;
  }

  let orderColumnFunction;
  switch (columnName) {
    case 'name':
      orderColumnFunction = (draft) => draft.name;
      break;
    case 'type':
      orderColumnFunction = (draft) => draft.artifact.name;
      break;
    case 'lastSaved':
      orderColumnFunction = (draft) =>
        draft.needsUpgrade ? draft.__ui__.lastSaved : draft.updatedTimeMillis;
      break;
  }

  const drafts = orderBy(state.list, [orderColumnFunction], [sortOrder]);

  Store.dispatch({
    type: Actions.setSort,
    payload: {
      sortColumn: columnName,
      sortOrder,
      list: drafts,
    },
  });
}
