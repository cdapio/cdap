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
import DataPrepHome from 'components/DataPrepHome';
import getPipelineConfig from 'components/DataPrep/TopPanel/PipelineConfigHelper';
import If from 'components/If';
import {
  IArtifactObj,
  PluginTypes,
  IProperties,
} from 'components/PipelineContextMenu/PipelineTypes';
import { GLOBALS } from 'services/global-constants';

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
  },
});

interface IPluginObj {
  name: string;
  artifact: IArtifactObj;
  label: string;
  type: PluginTypes;
  properties: IProperties;
}

interface INode {
  name: string;
  plugin: IPluginObj;
}

interface IConnection {
  from: string;
  to: string;
}

export type INewWranglerConnection = (obj: { nodes: INode[]; connections: IConnection[] }) => void;
interface IContextMenuOptionProp extends WithStyles<typeof styles> {
  onModalClose: () => void;
  onWranglerSourceAdd: INewWranglerConnection;
  pipelineArtifactType: 'cdap-data-pipeline' | 'cdap-data-streams';
}

function WranglerConnection({
  classes,
  onModalClose,
  onWranglerSourceAdd,
  pipelineArtifactType,
}: IContextMenuOptionProp) {
  const [showModal, setShowModal] = React.useState(true);
  const toggleModal = () => {
    setShowModal(!showModal);
    onModalClose();
  };
  const onWranglerConnectionSubmit = ({ onUnmount }) => {
    if (onUnmount) {
      return;
    }
    getPipelineConfig().subscribe(({ batchConfig, realtimeConfig }) => {
      let finalConfig;
      if (pipelineArtifactType === GLOBALS.etlDataPipeline) {
        finalConfig = batchConfig;
      }
      if (pipelineArtifactType === GLOBALS.etlDataStreams) {
        finalConfig = realtimeConfig;
      }
      if (!finalConfig) {
        return;
      }
      onWranglerSourceAdd({
        nodes: finalConfig.config.stages,
        connections: finalConfig.config.connections,
      });
      setShowModal(!showModal);
    });
  };
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
        <If condition={showModal}>
          <DataPrepHome singleWorkspaceMode={true} onSubmit={onWranglerConnectionSubmit} />
        </If>
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
  );
}

(WranglerConnectionWrapper as any).propTypes = {
  onModalClose: PropTypes.func,
  onWranglerSourceAdd: PropTypes.func,
  pipelineArtifactType: PropTypes.oneOf([GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams]),
};
