/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useContext } from 'react';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import LoadingSVG from 'components/LoadingSVG';
import ModalContent from 'components/FieldLevelLineage/v2/OperationsModal/ModalContent';
import IconSVG from 'components/IconSVG';
import withStyles from '@material-ui/core/styles/withStyles';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.OperationsModal';

require('../../OperationsModal/OperationsModal.scss');

const styles = () => {
  return {
    root: {
      maxWidth: '85vw',
      maxHeight: '80vh',
    },
  };
};

function OperationsModalView() {
  const { showOperations, toggleOperations, activeField, direction, loading } = useContext<
    IContextState
  >(FllContext);

  if (!showOperations) {
    return null;
  }

  const loadingIndicator = (
    <div className="loading-container text-center">
      <LoadingSVG />
    </div>
  );

  const closeModal = (e: React.MouseEvent<HTMLDivElement>) => {
    toggleOperations();
  };

  return (
    <Modal
      isOpen={true}
      toggle={toggleOperations}
      size="lg"
      backdrop="static"
      zIndex="1061"
      className="field-level-lineage-modal cdap-modal"
    >
      <ModalHeader>
        <span>{T.translate(`${PREFIX}.Title.${direction}`, { fieldName: activeField.name })}</span>

        <div className="close-section float-right" onClick={closeModal}>
          <IconSVG name="icon-close" />
        </div>
      </ModalHeader>

      <ModalBody>{loading ? loadingIndicator : <ModalContent />}</ModalBody>
    </Modal>
  );
}

const OperationsModal = withStyles(styles)(OperationsModalView);

export default OperationsModal;
