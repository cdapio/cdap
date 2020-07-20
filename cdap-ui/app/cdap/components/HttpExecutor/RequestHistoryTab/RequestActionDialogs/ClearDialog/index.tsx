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
    notifyRequestClear: () => {
      dispatch({
        type: HttpExecutorActions.notifyIncomingRequest,
        payload: {
          incomingRequest: {
            status: IncomingRequestStatus.CLEAR,
          },
        },
      });
    },
  };
};

interface IClearDialogProps {
  open: boolean;
  handleClose: () => void;
  notifyRequestClear: () => void;
}

const ClearDialogView: React.FC<IClearDialogProps> = ({
  open,
  handleClose,
  notifyRequestClear,
}) => {
  const clearRequestLog = () => {
    notifyRequestClear();
    handleClose();
  };

  return (
    <ConfirmationModal
      isOpen={open}
      headerTitle={'Clear all your request history'}
      confirmationElem={<div>Are you sure you want to clear all your request history?</div>}
      confirmButtonText={'Clear All'}
      confirmFn={clearRequestLog}
      cancelFn={handleClose}
    />
  );
};

const ClearDialog = connect(null, mapDispatch)(ClearDialogView);
export default ClearDialog;
