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
import { MySecureKeyApi } from 'api/securekey';
import React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';

interface ISecureKeyDeleteProps {
  state: any;
  open: boolean;
  handleClose: () => void;
  alertSuccess: () => void;
}

const SecureKeyDelete: React.FC<ISecureKeyDeleteProps> = ({
  state,
  open,
  handleClose,
  alertSuccess,
}) => {
  const { secureKeys, activeKeyIndex } = state;

  const deleteSecureKey = () => {
    const key = secureKeys.get(activeKeyIndex).get('name');

    const namespace = getCurrentNamespace();
    const params = {
      namespace,
      key,
    };

    MySecureKeyApi.delete(params).subscribe(() => {
      handleClose();
      alertSuccess();
    });
  };

  return (
    <ConfirmationModal
      headerTitle={'Delete secure key'}
      confirmationElem={'Are you sure you want to delete your secure key from your CDAP Account?'}
      confirmButtonText={'Delete'}
      confirmFn={deleteSecureKey}
      cancelFn={handleClose}
      isOpen={open}
    />
  );
};

export default SecureKeyDelete;
