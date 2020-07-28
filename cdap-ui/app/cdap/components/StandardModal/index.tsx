/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import { Modal, ModalBody, ModalFooter, ModalHeader } from 'reactstrap';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import { CSSTransition } from 'react-transition-group';
import IconSVG from 'components/IconSVG';
import React from 'react';
import classnames from 'classnames';

const styles = (): StyleRules => {
  return {
    modal: {
      position: 'relative',
      overflow: 'hidden',
    },
    modalHeaderText: {
      verticalAlign: 'top',
      paddingLeft: '6px',
    },
    floatRight: {
      float: 'right',
    },
    floatLeft: {
      float: 'left',
    },
    modalCloseBtn: {
      height: '30px',
      cursor: 'pointer',
    },
  };
};

interface IStandardModalProps extends WithStyles<typeof styles> {
  open: boolean;
  toggle: () => void;
  headerText: string;
  children: any;
}

const StandardModalView: React.FC<IStandardModalProps> = ({
  classes,
  open,
  toggle,
  headerText,
  children,
}) => {
  return (
    <Modal
      isOpen={open}
      toggle={toggle}
      className={classnames(classes.modal, 'cdap-modal')}
      size="lg"
      zIndex="1061"
      fade
    >
      <ModalHeader>
        <span className={classes.floatLeft}>
          <span className={classes.modalHeaderText}>{headerText}</span>
        </span>
        <div className={classes.floatRight}>
          <div className={classes.modalCloseBtn} onClick={toggle}>
            <IconSVG name="icon-close" />
          </div>
        </div>
      </ModalHeader>
      <ModalBody>
        <CSSTransition
          transitionName="plus-button-modal-content"
          transitionEnterTimeout={500}
          transitionLeaveTimeout={300}
          timeout={5000}
          component="div"
        >
          <div>{children}</div>
        </CSSTransition>
      </ModalBody>
    </Modal>
  );
};

const StandardModal = withStyles(styles)(StandardModalView);
export default StandardModal;
