/*
 * Copyright © 2021 Cask Data, Inc.
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

import { combineReducers, createStore, Store as InterfaceStore } from 'redux';
import PageTitleActions from './PageTitleActions';
import { composeEnhancers, objectQuery, isNilOrEmptyString } from 'services/helpers';
import { IAction } from 'services/redux-helpers';

interface IPageTitleStoreState {
  title: string;
}

const defaultInitialState = {
  title: '',
};

const defaultAction = {
  type: '',
  payload: {} as any,
};

const title = (state = '', action: IAction = defaultAction) => {
  switch (action.type) {
    case PageTitleActions.updatePageTitle:
      return action.payload.title;
    default:
      return state;
  }
};

const mutationObserver = new MutationObserver((mutations) => {
  if (!Array.isArray(mutations) || mutations.length === 0) {
    return;
  }
  const newTitle = objectQuery(mutations[0], 'target', 'textContent');
  if (isNilOrEmptyString(newTitle)) {
    return;
  }
  PageTitleStore.dispatch({ type: PageTitleActions.updatePageTitle, payload: { title: newTitle } });
});
mutationObserver.observe(document.querySelector('title'), { childList: true, subtree: true });

const PageTitleStore: InterfaceStore<IPageTitleStoreState> = createStore(
  combineReducers({ title }),
  defaultInitialState,
  composeEnhancers('PageTitleStore')()
);

const getCurrentPageTitle = () => {
  const { title: pageTitle } = PageTitleStore.getState();
  return pageTitle;
};

export default PageTitleStore;
export { getCurrentPageTitle };
