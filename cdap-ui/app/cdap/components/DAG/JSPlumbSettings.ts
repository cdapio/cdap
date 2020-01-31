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

export const defaultConnectionStyle = {
  hoverPaintStyle: {
    dashstyle: 'solid',
    lineWidth: 4,
    strokeStyle: '#58b7f6',
  },
  paintStyle: {
    lineWidth: 2,
    outlineColor: 'transparent',
    outlineWidth: 4,
    strokeStyle: '#4e5568',
  },
};

export const selectedConnectionStyle = {
  paintStyle: {
    dashstyle: 'solid',
    lineWidth: 4,
    outlineColor: 'transparent',
    outlineWidth: 4,
    strokeStyle: '#58b7f6',
  },
};

export const solidConnectionStyle = {
  paintStyle: { dashstyle: 'solid', strokeStyle: '#4e5568', strokeWidth: 4 },
};

export const dashedConnectionStyle = {
  paintStyle: { dashstyle: '2 4' },
};

export const conditionTrueConnectionStyle = {
  dashstyle: '2 4',
  lineWidth: 2,
  outlineColor: 'transparent',
  outlineWidth: 4,
  strokeStyle: '#0099ff',
};

export const conditionTrueEndpointStyle = {
  anchor: 'Right',
  connectorStyle: conditionTrueConnectionStyle,
  cssClass: 'condition-endpoint condition-endpoint-true',
  isSource: true,
  overlays: [
    [
      'Label',
      { label: 'Yes', id: 'yesLabel', location: [0.5, -0.55], cssClass: 'condition-label' },
    ],
  ],
};

export const conditionFalseConnectionStyle = {
  dashstyle: '2 4',
  lineWidth: 2,
  outlineColor: 'transparent',
  outlineWidth: 4,
  strokeStyle: '#999999',
};

export const conditionFalseEndpointStyle = {
  anchor: [0.5, 1, 0, 1, 2, 0], // same as Bottom but moved right 2px
  connectorStyle: conditionFalseConnectionStyle,
  cssClass: 'condition-endpoint condition-endpoint-false',
  isSource: true,
  overlays: [
    ['Label', { label: 'No', id: 'noLabel', location: [0.5, -0.55], cssClass: 'condition-label' }],
  ],
};

export const splitterEndpointStyle = {
  anchor: 'Right',
  cssClass: 'splitter-endpoint',
  isSource: true,
};

export const alertEndpointStyle = {
  anchor: [0.5, 1, 0, 1, 2, 0], // same as Bottom but moved right 2px
  scope: 'alertScope',
};

export const errorEndpointStyle = {
  anchor: [0.5, 1, 0, 1, 3, 0], // same as Bottom but moved right 3px
  scope: 'errorScope',
};

export const targetNodeOptions = {
  allowLoopback: false,
  anchor: 'ContinuousLeft',
  dropOptions: { hoverClass: 'drag-hover' },
  isTarget: true,
};

export const defaultJsPlumbSettings = {
  Anchor: [1, 0.5, 1, 0, 0, 2], // same as Right but moved down 2px
  ConnectionOverlays: [
    [
      'Arrow',
      {
        foldback: 0.8,
        id: 'arrow',
        length: 14,
        location: 1,
      },
    ],
  ],
  Connector: [
    'Flowchart',
    { stub: [10, 15], alwaysRespectStubs: true, cornerRadius: 20, midpoint: 0.2 },
  ],
  Container: 'dag-container',
  Endpoint: 'Dot',
  EndpointStyle: {
    fill: 'black',
    lineWidth: 3,
    radius: 5,
    stroke: 'black',
  },
  MaxConnections: -1,
  ...defaultConnectionStyle,
};
