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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ConfirmationModal from 'components/ConfirmationModal';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import If from 'components/If';

const styles = (): StyleRules => {
  return {
    replicatorName: {
      marginLeft: '5px',
    },
  };
};

export enum InstanceType {
  app = 'app',
  draft = 'draft',
}

interface IDeleteConfirmationView extends WithStyles<typeof styles> {
  replicatorId: string;
  onDelete: () => void;
  show: boolean;
  closeModal: () => void;
  type: InstanceType;
}

const DeleteConfirmationView: React.FC<IDeleteConfirmationView> = ({
  classes,
  replicatorId,
  onDelete,
  show,
  closeModal,
  type = InstanceType.draft,
}) => {
  const [error, setError] = React.useState();
  if (!show) {
    return null;
  }

  function handleDelete() {
    const appParams = {
      namespace: getCurrentNamespace(),
      appName: replicatorId,
    };

    let api = MyReplicatorApi.delete(appParams);

    if (type === InstanceType.draft) {
      const draftParams = {
        namespace: getCurrentNamespace(),
        draftId: replicatorId,
      };
      api = MyReplicatorApi.deleteDraft(draftParams);
    }

    api.subscribe(
      () => {
        closeModal();
        onDelete();
      },
      (err) => {
        setError(err);
      }
    );
  }

  function cancel() {
    setError(null);
    closeModal();
  }

  const confirmElem = (
    <div>
      Are you sure you want to delete
      <If condition={type === InstanceType.app}>
        <strong className={classes.replicatorName}>
          <em>{replicatorId}</em>
        </strong>
      </If>
      ?
    </div>
  );

  return (
    <ConfirmationModal
      headerTitle={
        type === InstanceType.app
          ? 'Delete replication pipeline'
          : 'Delete replication pipeline draft'
      }
      toggleModal={closeModal}
      confirmationElem={confirmElem}
      confirmButtonText="Delete"
      confirmFn={handleDelete}
      cancelFn={cancel}
      isOpen={show}
      errorMessage={!error ? '' : 'Failed to delete replication pipeline'}
      extendedMessage={error}
    />
  );
};

const DeleteConfirmation = withStyles(styles)(DeleteConfirmationView);
export default DeleteConfirmation;
