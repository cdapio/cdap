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
import React from 'react';
import { connect } from 'react-redux';

const mapDispatch = (dispatch) => {
  return {
    clearAllRequestLog: () => {
      dispatch({
        type: HttpExecutorActions.clearAllRequestLog,
      });
    },
  };
};

interface IClearAllDialogProps {
  open: boolean;
  handleClose: () => void;
  clearAllRequestLog: () => void;
}

const ClearAllDialogView: React.FC<IClearAllDialogProps> = ({
  open,
  handleClose,
  clearAllRequestLog,
}) => {
  const onClearAllClick = () => {
    clearAllRequestLog();
    handleClose();
  };

  return (
    <ConfirmationModal
      isOpen={open}
      headerTitle={'Clear all your request history'}
      confirmationElem={<div>Are you sure you want to clear all your request history?</div>}
      confirmButtonText={'Clear All'}
      confirmFn={onClearAllClick}
      cancelFn={handleClose}
    />
  );
};

const ClearAllDialog = connect(null, mapDispatch)(ClearAllDialogView);
export default ClearAllDialog;
