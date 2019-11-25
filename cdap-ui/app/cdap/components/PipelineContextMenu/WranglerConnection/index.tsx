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

import React from 'react';
import { Modal, ModalBody } from 'reactstrap';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import IconSVG from 'components/IconSVG';
import PropTypes from 'prop-types';

const styles = (theme) => ({
  modalBtnClose: {
    height: '50px',
    width: '50px',
    boxShadow: 'none',
    border: 0,
    background: 'transparent',
    borderLeft: `1px solid ${theme.palette.grey['300']}`,
    fontWeight: 'bold' as 'bold',
    fontSize: '1.5rem',
    '&:hover': {
      background: theme.palette.blue['40'],
      color: 'white',
    },
  }
});

interface IContextMenuOptionProp extends WithStyles<typeof styles> {
  onModalClose: () => void
}

function WranglerConnection({ classes, onModalClose }: IContextMenuOptionProp) {
  const [showModal, setShowModal] = React.useState(true);
  const toggleModal = () => { setShowModal(!showModal); onModalClose(); }
  return (
    <Modal
      isOpen={showModal}
      toggle={toggleModal}
      size="lg"
      modalClassName="wrangler-modal"
      backdrop="static"
      zIndex="1061"
    >
      <div className="modal-header">
        <h5 className="modal-title">Wrangle</h5>
        <button className={classes.modalBtnClose} onClick={toggleModal}>
          <IconSVG name="icon-close" />
        </button>
      </div>
      <div className="modal-body">
        <h1> Coming soon ... </h1>
      </div>
    </Modal>
  );
}

const StyledWranglerConnection = withStyles(styles)(WranglerConnection);

export default function WranglerConnectionWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledWranglerConnection {...props} />
    </ThemeWrapper>
  )
}

(WranglerConnectionWrapper as any).propTypes = {
  onModalClose: PropTypes.func
}
