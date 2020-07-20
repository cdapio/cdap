/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import { List, fromJS } from 'immutable';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Alert from 'components/Alert';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { MySecureKeyApi } from 'api/securekey';
import SecureKeyDelete from 'components/SecureKeys/SecureKeyDelete';
import SecureKeyEdit from 'components/SecureKeys/SecureKeyEdit';
import SecureKeyList from 'components/SecureKeys/SecureKeyList';
import { getCurrentNamespace } from 'services/NamespaceStore';

interface ISecureKeyState {
  createdEpochMs?: number;
  description: string;
  name: string;
  properties: Record<string, string>;
  data: string;
}

export enum SecureKeyStatus {
  Normal = 'Normal',
  Success = 'SUCCESS',
  Failure = 'FAILURE',
}

export const initialState = {
  secureKeys: List([]),
  secureKeyStatus: SecureKeyStatus.Normal,
  editMode: false,
  deleteMode: false,
  activeKeyIndex: null,
  loading: true,
};

export function reducer(state, action) {
  switch (action.type) {
    case 'SET_SECURE_KEYS':
      return { ...state, secureKeys: action.secureKeys };
    case 'SET_SECURE_KEY_STATUS':
      return { ...state, secureKeyStatus: action.secureKeyStatus };
    case 'SET_EDIT_MODE':
      return { ...state, editMode: action.editMode };
    case 'SET_DELETE_MODE':
      return { ...state, deleteMode: action.deleteMode };
    case 'SET_ACTIVE_KEY_INDEX':
      return { ...state, activeKeyIndex: action.activeKeyIndex };
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

  const { secureKeyStatus, editMode, deleteMode, loading } = state;

  const namespace = getCurrentNamespace();

  React.useEffect(() => {
    fetchSecureKeys();
  }, []);

  const fetchSecureKeys = () => {
    // Update the states with incoming secure keys.
    // Whenever user adds/edits/deletes a secure key, securekeyStatus emits a new value.
    // In such case, re-call MySecureKeyApi.list to reflect the changes
    MySecureKeyApi.list({ namespace }).subscribe((keys: ISecureKeyState[]) => {
      dispatch({ type: 'SET_LOADING', loading: true });
      if (!keys) {
        return;
      }

      // Populate the table with matched secure keys
      dispatch({ type: 'SET_SECURE_KEYS', secureKeys: fromJS(keys) });
      dispatch({ type: 'SET_LOADING', loading: false });
    });
  };

  // Success Alert component always closes after 3000ms.
  // Failure Alert component status closes only when user manually closes it.
  // Reset status when an Alert component is closed.
  const onAlertClose = () => {
    dispatch({ type: 'SET_SECURE_KEY_STATUS', secureKeyStatus: SecureKeyStatus.Normal });
  };

  const onEditDialogClose = () => {
    dispatch({ type: 'SET_EDIT_MODE', editMode: false });
    dispatch({ type: 'SET_ACTIVE_KEY_INDEX', activeKeyIndex: null });
  };

  const onDeleteDialogClose = () => {
    dispatch({ type: 'SET_DELETE_MODE', deleteMode: false });
    dispatch({ type: 'SET_ACTIVE_KEY_INDEX', activeKeyIndex: null });
  };

  const alertSuccess = () => {
    dispatch({ type: 'SET_SECURE_KEY_STATUS', secureKeyStatus: SecureKeyStatus.Success });

    fetchSecureKeys();
  };

  const alertFailure = () => {
    dispatch({ type: 'SET_SECURE_KEY_STATUS', secureKeyStatus: SecureKeyStatus.Failure });
  };

  const openDeleteDialog = (keyIndex) => {
    dispatch({ type: 'SET_DELETE_MODE', deleteMode: true });
    dispatch({ type: 'SET_ACTIVE_KEY_INDEX', activeKeyIndex: keyIndex });
  };

  const openEditDialog = (keyIndex) => {
    dispatch({ type: 'SET_EDIT_MODE', editMode: true });
    dispatch({ type: 'SET_ACTIVE_KEY_INDEX', activeKeyIndex: keyIndex });
  };

  return (
    <div className="container">
      <Alert
        message={'saved successfully'}
        showAlert={secureKeyStatus === SecureKeyStatus.Success}
        type="success"
        onClose={onAlertClose}
      />
      <Alert
        message={'Error: Duplicate key name'}
        showAlert={secureKeyStatus === SecureKeyStatus.Failure}
        type="error"
        onClose={onAlertClose}
      />

      <div className={classes.content}>
        <If condition={loading}>
          <div className={classes.loadingBox}>
            <LoadingSVGCentered />
          </div>
        </If>
        <If condition={!loading}>
          <SecureKeyList
            state={state}
            alertSuccess={alertSuccess}
            alertFailure={alertFailure}
            openEditDialog={openEditDialog}
            openDeleteDialog={openDeleteDialog}
          />
        </If>
      </div>

      <If condition={editMode}>
        <SecureKeyEdit
          state={state}
          open={editMode}
          handleClose={onEditDialogClose}
          alertSuccess={alertSuccess}
        />
      </If>
      <If condition={deleteMode}>
        <SecureKeyDelete
          state={state}
          open={deleteMode}
          handleClose={onDeleteDialogClose}
          alertSuccess={alertSuccess}
        />
      </If>
    </div>
  );
};

const SecureKeys = withStyles(styles)(SecureKeysView);
export default SecureKeys;
