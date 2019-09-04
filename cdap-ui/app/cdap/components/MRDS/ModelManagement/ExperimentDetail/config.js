import { isNil } from 'lodash';
import { timeFormatter } from '../utils/commonUtils';

function datasetShemaNameRenderer(data) {
  if (!isNil(data) && !isNil(data.data)) {
    if (!isNil(data.data.featureORprediction) && data.data.featureORprediction != "") {
      return `<span class='feature-prediction'>${data.data.featureORprediction}</span>`;
    } else {
      return "";
    }
  } else {
    return "";
  }
}

function modelLinkRenderer(params) {
  var eDiv = document.createElement('div');
  eDiv.innerHTML = params.value;
  eDiv.classList.add("view-link");
  return eDiv;
}

function modelStatusRenderer(data) {
  let className = '';
  if (data.value.toLowerCase() === 'inactive') {
    className = 'inactive-status';
  } else if (data.value.toLowerCase() === 'discoverable') {
    className = 'running-status';
  } else if (data.value.toLowerCase() === 'deployed') {
    className = 'active-status';
  }
  return `<span class='${className}'>${data.value}</span>`;
}


export const DATASET_SCHEMA_LIST_COLUMN_DEF = [
  { headerName: "", field: "featureORprediction", width: 20, cellRenderer: datasetShemaNameRenderer.bind(this) },
  { headerName: "Column Name", field: "name", width: 100, tooltipField: "name" },
  { headerName: "Type", field: "type", width: 60 },
];

export const MODEL_DETAIL_COLUMN_DEF = [
  { headerName: "Creation Date", field: "createtime", valueFormatter: timeFormatter, width: 150, resizable: true },
  { headerName: "Version", field: "modelId", tooltipField: "modelId", cellRenderer: modelLinkRenderer, width: 300, resizable: true },
  { headerName: "Status", field: "modelStatus", cellRenderer: modelStatusRenderer.bind(this), width: 100, resizable: true },
  {
    headerName: "Hyper-parameter",
    children: [],
    resizable: true
  }, {
    headerName: "Metrics",
    children: [],
    resizable: true
  }
];
