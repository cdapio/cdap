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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import { MySecureKeyApi } from 'api/securekey';
import Alert from 'components/Alert';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import SecureKeyDelete from 'components/SecureKeys/SecureKeyDelete';
import SecureKeyDetails from 'components/SecureKeys/SecureKeyDetails';
import SecureKeyEdit from 'components/SecureKeys/SecureKeyEdit';
import SecureKeyList from 'components/SecureKeys/SecureKeyList';
import { fromJS, List, Map } from 'immutable';
import * as React from 'react';
import { BehaviorSubject } from 'rxjs';
import { forkJoin } from 'rxjs/observable/forkJoin';
import { distinctUntilChanged, flatMap, mergeMap } from 'rxjs/operators';
import { map } from 'rxjs/operators/map';
import { getCurrentNamespace } from 'services/NamespaceStore';

const COMMON_DELIMITER = ',';
const COMMON_KV_DELIMITER = ';';

interface ISecureKeyState {
  createdEpochMs?: number;
  description: string;
  name: string;
  properties: Record<string, string>;
  data: string;
}

export enum SecureKeysPageMode {
  List = 'LIST',
  Details = 'DETAILS',
}

export enum SecureKeyStatus {
  Normal = 'Normal',
  Success = 'SUCCESS',
  Failure = 'FAILURE',
}

export const initialState = {
  secureKeys: List([]),
  visibilities: Map<string, boolean>({}),
  secureKeyStatus: SecureKeyStatus.Normal,
  pageMode: SecureKeysPageMode.List,
  editMode: false,
  deleteMode: false,
  activeKeyIndex: null,
  searchText: '',
  loading: true,
};

export function reducer(state, action) {
  switch (action.type) {
    case 'SET_SECURE_KEYS':
      return { ...state, secureKeys: action.secureKeys };
    case 'SET_VISIBILITIES':
      return { ...state, visibilities: action.visibilities };
    case 'SET_VISIBILITY':
      const { visibilities, keyID } = action;
      return { ...state, visibilities: visibilities.set(keyID, !visibilities.get(keyID)) };
    case 'SET_SECURE_KEY_STATUS':
      return { ...state, secureKeyStatus: action.secureKeyStatus };
    case 'SET_PAGE_MODE':
      return { ...state, pageMode: action.pageMode };
    case 'SET_EDIT_MODE':
      return { ...state, editMode: action.editMode };
    case 'SET_DELETE_MODE':
      return { ...state, deleteMode: action.deleteMode };
    case 'SET_ACTIVE_KEY_INDEX':
      return { ...state, activeKeyIndex: action.activeKeyIndex };
    case 'SET_SEARCH_TEXT':
      return { ...state, searchText: action.searchText };
    case 'SET_LOADING':
      return { ...state, loading: action.loading };
    default:
      return state;
  }
}

const styles = (): StyleRules => {
  return {
    content: {
      padding: '50px',
    },
    loadingBox: {
      width: '100%',
      height: '100%',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
  };
};

interface ISecureKeysProps extends WithStyles<typeof styles> {}

const SecureKeysView: React.FC<ISecureKeysProps> = ({ classes }) => {
  const [state, dispatch] = React.useReducer(reducer, initialState);

  const {
    visibilities,
    secureKeyStatus,
    pageMode,
    editMode,
    deleteMode,
    searchText,
    loading,
  } = state;

  const namespace = getCurrentNamespace();

  // Observe `searchText` with `useEffect` and forward the value to `searchText$`
  const searchTextSubject = React.useRef(new BehaviorSubject(searchText));
  React.useEffect(() => {
    searchTextSubject.current.next(searchText);
  }, [searchText]);
  const searchText$ = React.useMemo(() => searchTextSubject.current.asObservable(), [
    searchTextSubject,
  ]);

  // Observe `secureKeyStatus` with `useEffect` and forward the value to `secureKeyStatus$`
  const secureKeyStatusSubject = React.useRef(new BehaviorSubject(secureKeyStatus));
  React.useEffect(() => {
    secureKeyStatusSubject.current.next(secureKeyStatus);
  }, [secureKeyStatus]);
  const secureKeyStatus$ = React.useMemo(() => secureKeyStatusSubject.current.asObservable(), [
    secureKeyStatusSubject,
  ]);

  // Update the states with incoming secure keys
  React.useEffect(() => {
    // Whenever user adds/edits/deletes a secure key, securekeyStatus$ emits a new value.
    // In such case, re-call MySecureKeyApi.list to reflect the changes
    const secureKeys$ = secureKeyStatus$
      .pipe(
        distinctUntilChanged(),
        flatMap((status) => {
          return MySecureKeyApi.list({ namespace }).pipe(
            mergeMap((keys: ISecureKeyState[]) => {
              return forkJoin(
                keys.map((k) =>
                  MySecureKeyApi.getSecureData({ namespace, key: k.name }).pipe(
                    map((data: string) => {
                      k.data = data;
                      return k;
                    })
                  )
                )
              );
            })
          );
        })
      )
      .publishReplay(1)
      .refCount();

    // When a user searches for specific secure keys, filter them down
    const filteredKeys$ = searchText$.pipe(
      distinctUntilChanged(),
      flatMap((searchtext) => {
        return secureKeys$.pipe(
          map((keys: any[]) => {
            if (!keys) {
              return [];
            }
            return keys.filter(
              (key) => key.name.includes(searchtext) || key.description.includes(searchtext)
            );
          })
        );
      })
    );

    const subscription = filteredKeys$.subscribe((keys: ISecureKeyState[]) => {
      dispatch({ type: 'SET_LOADING', loading: true });
      if (!keys) {
        return;
      }

      // Populate the table with matched secure keys
      dispatch({ type: 'SET_SECURE_KEYS', secureKeys: fromJS(keys) });

      const newVisibilities = {};
      keys.forEach(({ name }) => {
        // If the secure key alrady exists, do not override visibility.
        // Otherwise, initialize it to 'false'
        if (visibilities.has(name)) {
          newVisibilities[name] = visibilities.get(name);
        } else {
          newVisibilities[name] = false;
        }
      });
      dispatch({ type: 'SET_VISIBILITIES', visibilities: Map(newVisibilities) });
      dispatch({ type: 'SET_LOADING', loading: false });
    });

    return () => {
      return subscription.unsubscribe();
    };
  }, []);

  // Success Alert component always closes after 3000ms.
  // After a timeout for 3000ms, reset status to make a success Alert component disappear
  const onSuccessAlertClose = () => {
    dispatch({ type: 'SET_SECURE_KEY_STATUS', secureKeyStatus: SecureKeyStatus.Normal });
  };

  // A failure Alert component status should close only when user manually closes it
  const onFailureAlertClose = () => {
    dispatch({ type: 'SET_SECURE_KEY_STATUS', secureKeyStatus: SecureKeyStatus.Normal });
  };

  return (
    <div className="container">
      <Alert
        message={'saved successfully'}
        showAlert={secureKeyStatus === SecureKeyStatus.Success}
        type="success"
        onClose={onSuccessAlertClose}
      />
      <Alert
        message={'Error: Duplicate key name'}
        showAlert={secureKeyStatus === SecureKeyStatus.Failure}
        type="error"
        onClose={onFailureAlertClose}
      />

      <div className={classes.content}>
        <If condition={loading}>
          <div className={classes.loadingBox}>
            <LoadingSVGCentered />
          </div>
        </If>
        <If condition={!loading && pageMode === SecureKeysPageMode.List}>
          <SecureKeyList state={state} dispatch={dispatch} />
        </If>
        <If condition={!loading && pageMode === SecureKeysPageMode.Details}>
          <SecureKeyDetails state={state} dispatch={dispatch} />
        </If>
      </div>

      <If condition={editMode}>
        <SecureKeyEdit
          state={state}
          dispatch={dispatch}
          open={editMode}
          handleClose={() => dispatch({ type: 'SET_EDIT_MODE', editMode: false })}
        />
      </If>
      <If condition={deleteMode}>
        <SecureKeyDelete
          state={state}
          dispatch={dispatch}
          open={deleteMode}
          handleClose={() => dispatch({ type: 'SET_DELETE_MODE', deleteMode: false })}
        />
      </If>
    </div>
  );
};

const SecureKeys = withStyles(styles)(SecureKeysView);
export default SecureKeys;
