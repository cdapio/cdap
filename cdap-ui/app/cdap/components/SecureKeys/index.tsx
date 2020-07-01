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
import SecureKeyDelete from 'components/SecureKeys/SecureKeyDelete';
import SecureKeyEdit from 'components/SecureKeys/SecureKeyEdit';
import SecureKeyList from 'components/SecureKeys/SecureKeyList';
import { fromJS, List, Map } from 'immutable';
import * as React from 'react';
import { BehaviorSubject } from 'rxjs';
import { forkJoin } from 'rxjs/observable/forkJoin';
import { flatMap, mergeMap } from 'rxjs/operators';
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

export enum SecureKeyStatus {
  Normal = 'Normal',
  Success = 'SUCCESS',
  Failure = 'FAILURE',
}

const styles = (): StyleRules => {
  return {
    content: {
      padding: '50px',
    },
  };
};

interface ISecureKeysProps extends WithStyles<typeof styles> {}

const SecureKeysView: React.FC<ISecureKeysProps> = ({ classes }) => {
  const [secureKeys, setSecureKeys] = React.useState(List([]));
  const [visibility, setVisibility] = React.useState(Map<string, boolean>({}));

  const [loading, setLoading] = React.useState(true);
  const [secureKeyStatus, setSecureKeyStatus] = React.useState(SecureKeyStatus.Normal);
  const [editMode, setEditMode] = React.useState(false);
  const [deleteMode, setDeleteMode] = React.useState(false);
  const [activeKeyIndex, setActiveKeyIndex] = React.useState(null);

  const fetchSecureKeys = () => {
    const namespace = getCurrentNamespace();

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
  };

  // Observe `secureKeyStatus` with `useEffect` and forward the value to `secureKeyStatus$`
  const secureKeyStatusSubject = React.useRef(new BehaviorSubject(secureKeyStatus));
  React.useEffect(() => {
    secureKeyStatusSubject.current.next(secureKeyStatus);
  }, [secureKeyStatus]);
  const secureKeyStatus$ = React.useMemo(() => secureKeyStatusSubject.current.asObservable(), [
    secureKeyStatusSubject,
  ]);

  // When a user filters secure keys or create/edit/delete secure keys,
  // fetch new secure keys
  const searchResults$ = secureKeyStatus$.pipe(
    flatMap((status: SecureKeyStatus) => {
      return fetchSecureKeys().pipe(
        map((keys: any[]) => {
          return keys;
        })
      );
    })
  );

  // Update the states with incoming secure keys
  React.useEffect(() => {
    const subscription = searchResults$.subscribe((keys: ISecureKeyState[]) => {
      setLoading(true);
      // Populate the table with matched secure keys
      setSecureKeys(fromJS(keys));
      const newVisibility = {};
      keys.forEach(({ name }) => {
        // If the secure key alrady exists, do not override visibility.
        // Otherwise, initialize it to 'false'
        if (visibility.has(name)) {
          newVisibility[name] = visibility.get(name);
        } else {
          newVisibility[name] = false;
        }
      });
      setVisibility(Map(newVisibility));
      setLoading(false);
    });

    return () => {
      return subscription.unsubscribe();
    };
  }, []);

  // Success Alert component always closes after 3000ms.
  // After a timeout for 3000ms, reset status to make a success Alert component disappear
  const onSuccessAlertClose = () => {
    setSecureKeyStatus(SecureKeyStatus.Normal);
  };

  // A failure Alert component status should close only when user manually closes it
  const onFailureAlertClose = () => {
    setSecureKeyStatus(SecureKeyStatus.Normal);
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
        <SecureKeyList
          secureKeys={secureKeys}
          setSecureKeyStatus={setSecureKeyStatus}
          setActiveKeyIndex={setActiveKeyIndex}
          setEditMode={setEditMode}
          setDeleteMode={setDeleteMode}
          visibility={visibility}
          setVisibility={setVisibility}
          loading={loading}
        />
      </div>

      <If condition={editMode}>
        <SecureKeyEdit
          open={editMode}
          handleClose={() => setEditMode(false)}
          keyMetadata={secureKeys.get(activeKeyIndex)}
          setSecureKeyStatus={setSecureKeyStatus}
        />
      </If>
      <If condition={deleteMode}>
        <SecureKeyDelete
          open={deleteMode}
          handleClose={() => setDeleteMode(false)}
          secureKeys={secureKeys}
          activeKeyIndex={activeKeyIndex}
          setActiveKeyIndex={setActiveKeyIndex}
          setSecureKeyStatus={setSecureKeyStatus}
        />
      </If>
    </div>
  );
};

const SecureKeys = withStyles(styles)(SecureKeysView);
export default SecureKeys;
