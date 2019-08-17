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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { IWidgetProps } from '..';
import CodeEditor from 'components/CodeEditor';
import Button from '@material-ui/core/Button';
import IconSVG from 'components/IconSVG';
import If from 'components/If';
import { Modal, ModalBody } from 'reactstrap';
import RulesEngineHome from 'components/RulesEngineHome';
import { objectQuery } from 'services/helpers';
import './rules-engine-modal.scss';

const styles = (theme): StyleRules => {
  return {
    root: {
      paddingTop: '7px',
    },
    button: {
      margin: '10px 0',
    },
    btnIcon: {
      marginRight: '5px',
    },
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
    },
  };
};

interface IRulesEngineProps extends IWidgetProps<null>, WithStyles<typeof styles> {}

const RulesEngineEditorView: React.FC<IRulesEngineProps> = ({
  value,
  onChange,
  updateAllProperties,
  extraConfig,
  disabled,
  classes,
}) => {
  const [showModal, setShowModal] = React.useState<boolean>(false);
  const rulebookIdProp = objectQuery(extraConfig, 'properties', 'rulebookid');

  function onApply(rulebook, rulebookid) {
    if (rulebook) {
      updateAllProperties({
        rulebook,
        rulebookid,
      });
    }

    setShowModal(false);
  }

  return (
    <React.Fragment>
      <div className={classes.root}>
        <Button
          className={classes.button}
          variant="contained"
          color="primary"
          onClick={() => setShowModal(true)}
          disabled={disabled}
        >
          <IconSVG name="icon-DataPreparation" className={classes.btnIcon} />
          Rules
        </Button>
        <CodeEditor mode="scala" value={value} onChange={onChange} rows={25} disabled={disabled} />
      </div>

      <If condition={showModal}>
        <Modal
          isOpen={showModal}
          toggle={() => setShowModal(!showModal)}
          size="lg"
          modalClassName="rules-engine-modal"
          backdrop="static"
          zIndex="1061"
        >
          <div className="modal-header">
            <h5 className="modal-title">Rules Engine</h5>
            <button className={classes.modalBtnClose} onClick={() => setShowModal(false)}>
              <IconSVG name="icon-close" />
            </button>
          </div>
          <ModalBody>
            <RulesEngineHome embedded={true} rulebookid={rulebookIdProp} onApply={onApply} />
          </ModalBody>
        </Modal>
      </If>
    </React.Fragment>
  );
};

const RulesEngineEditor = withStyles(styles)(RulesEngineEditorView);
export default RulesEngineEditor;
