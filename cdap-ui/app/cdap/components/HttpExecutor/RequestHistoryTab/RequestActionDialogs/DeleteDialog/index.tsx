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

import ConfirmationModal from 'components/ConfirmationModal';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import { IncomingRequestStatus } from 'components/HttpExecutor/RequestHistoryTab';
import React from 'react';
import { connect } from 'react-redux';

const mapDispatch = (dispatch) => {
  return {
    notifyRequestDelete: (requestID: Date) => {
      dispatch({
        type: HttpExecutorActions.notifyIncomingRequest,
        payload: {
          incomingRequest: {
            status: IncomingRequestStatus.DELETE,
            requestID,
          },
        },
      });
    },
  };
};

interface IDeleteDialogProps {
  requestID: Date;
  open: boolean;
  handleClose: () => void;
  notifyRequestDelete: (requestID: Date) => void;
}

const DeleteDialogView: React.FC<IDeleteDialogProps> = ({
  requestID,
  open,
  handleClose,
  notifyRequestDelete,
}) => {
  const deleteRequestLog = (id: Date) => {
    notifyRequestDelete(id);
    handleClose();
  };

  return (
    <ConfirmationModal
      isOpen={open}
      headerTitle={'Delete your request history'}
      confirmationElem={<div>Are you sure you want to delete your request history?</div>}
      confirmButtonText={'Delete'}
      confirmFn={() => deleteRequestLog(requestID)}
      cancelFn={handleClose}
    />
  );
};

const DeleteDialog = connect(null, mapDispatch)(DeleteDialogView);
export default DeleteDialog;
