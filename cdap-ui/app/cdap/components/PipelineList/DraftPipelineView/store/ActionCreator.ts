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
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';
import Store, { Actions } from 'components/PipelineList/DraftPipelineView/store';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';

const DRAFTS_KEY = 'hydratorDrafts';
const PROPERTY = 'property';

export function getDrafts() {
  MyUserStoreApi.get().subscribe((res) => {
    const namespace = getCurrentNamespace();
    const draftsObj = objectQuery(res, PROPERTY, DRAFTS_KEY, namespace) || {};

    const drafts: IDraft[] = [];

    Object.keys(draftsObj).forEach((id) => {
      drafts.push(draftsObj[id]);
    });

    Store.dispatch({
      type: Actions.setDrafts,
      payload: {
        list: drafts,
      },
    });
  });
}

export function deleteDraft(draft: IDraft) {
  const draftId = draft.__ui__.draftId;

  MyUserStoreApi.get().subscribe((res) => {
    const namespace = getCurrentNamespace();
    const draftObj = objectQuery(res, PROPERTY, DRAFTS_KEY, namespace, draftId);

    if (draftObj) {
      delete res.property[DRAFTS_KEY][namespace][draftId];

      MyUserStoreApi.set(null, res.property).subscribe(getDrafts);
    }
  });
}
