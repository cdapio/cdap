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
import { ContextMenu, IContextMenuOption } from 'components/ContextMenu';
import WranglerConnection from 'components/PipelineContextMenu/WranglerConnection';
import If from 'components/If';
import PropTypes from 'prop-types';
import { CopyFromClipBoard } from 'services/Clipboard';
import { objectQuery } from 'services/helpers';
import { INode, IConnection } from 'components/PipelineContextMenu/PipelineTypes';
import { INewWranglerConnection } from 'components/PipelineContextMenu/WranglerConnection';
import { GLOBALS } from 'services/global-constants';

export interface IStage {
  nodes: INode[];
  connections: IConnection[];
}
interface IPipelineContextMenuProps {
  onNodesPaste: (stages: IStage) => void;
  onWranglerSourceAdd: INewWranglerConnection;
  pipelineArtifactType: 'cdap-data-pipeline' | 'cdap-data-streams';
  onZoomIn: () => void;
  onZoomOut: () => void;
  fitToScreen: () => void;
  prettyPrintGraph: () => void;
}

async function getNodesFromClipBoard(): Promise<IStage | undefined> {
  let clipText;
  try {
    clipText = await CopyFromClipBoard();
  } catch (e) {
    return Promise.reject();
  }
  return Promise.resolve(getClipboardData(clipText));
}

function getClipboardData(text): IStage | undefined {
  let config;
  if (typeof text !== 'object') {
    try {
      config = JSON.parse(text);
      config.nodes = config.stages;
      delete config.stages;
    } catch (e) {
      return;
    }
  }
  return config;
}

async function isPasteOptionDisabled(): Promise<boolean> {
  let clipText;
  try {
    clipText = await CopyFromClipBoard();
  } catch (e) {
    return Promise.reject(true);
  }
  return Promise.resolve(isClipboardPastable(clipText));
}

function isClipboardPastable(text) {
  let jsonNodes;
  if (typeof text !== 'object') {
    try {
      jsonNodes = JSON.parse(text);
    } catch (e) {
      return true;
    }
  }
  return objectQuery(jsonNodes, 'stages', 'length') > 0 ? false : true;
}

export default function PipelineContextMenu({
  onWranglerSourceAdd,
  onNodesPaste,
  pipelineArtifactType,
  onZoomIn,
  onZoomOut,
  fitToScreen,
  prettyPrintGraph,
}: IPipelineContextMenuProps) {
  const [showWranglerModal, setShowWranglerModal] = React.useState(false);
  const [pasteOptionDisabled, setPasteOptionDisabled] = React.useState(true);

  isPasteOptionDisabled().then(setPasteOptionDisabled);

  const updateOptionDisabledFlags = () => {
    isPasteOptionDisabled().then(setPasteOptionDisabled);
  };
  const menuOptions: IContextMenuOption[] = [
    {
      name: 'add-wrangler-source',
      label: 'Add a wrangler source',
      onClick: () => setShowWranglerModal(!showWranglerModal),
    },
    {
      name: 'zoom-in',
      label: 'Zoom in',
      onClick: onZoomIn,
    },
    {
      name: 'zoom-out',
      label: 'Zoom Out',
      onClick: onZoomOut,
    },
    {
      name: 'fit-to-screen',
      label: 'Fit to Screen',
      onClick: fitToScreen,
    },
    {
      name: 'align-nodes',
      label: 'Pretty print',
      onClick: prettyPrintGraph,
    },
    {
      name: 'pipeline-node-paste',
      label: 'Paste',
      onClick: () => {
        getNodesFromClipBoard().then(onNodesPaste);
      },
      disabled: pasteOptionDisabled,
    },
  ];
  const onWranglerSourceAddWrapper = (...props) => {
    setShowWranglerModal(!showWranglerModal);
    onWranglerSourceAdd.apply(null, props);
  };
  return (
    <React.Fragment>
      <ContextMenu
        selector="#diagram-container"
        options={menuOptions}
        onOpen={updateOptionDisabledFlags}
      />
      <If condition={showWranglerModal}>
        <WranglerConnection
          onModalClose={() => setShowWranglerModal(!showWranglerModal)}
          onWranglerSourceAdd={onWranglerSourceAddWrapper}
          pipelineArtifactType={pipelineArtifactType}
        />
      </If>
    </React.Fragment>
  );
}

(PipelineContextMenu as any).propTypes = {
  onWranglerSourceAdd: PropTypes.func,
  onNodesPaste: PropTypes.func,
  pipelineArtifactType: PropTypes.oneOf([GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams]),
  onZoomIn: PropTypes.func,
  onZoomOut: PropTypes.func,
  fitToScreen: PropTypes.func,
  prettyPrintGraph: PropTypes.func,
};
