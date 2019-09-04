import * as momentTimeZone from 'moment-timezone';
import { isNil } from 'lodash';

function authorRenderer() {
  return "admin";
}

function timeFormatter(params) {
  let dateTimeFormat = 'DD-MM-YYYY HH:mm';
  let date = getMomentDateFor(params.value, 'GMT');
  return isNil(date) ? "" : date.format(dateTimeFormat);
}



function getMomentDateFor(epoch, timezone) {
  if (epoch === undefined) {
    return undefined;
  }
  return momentTimeZone.tz(epoch, timezone);
}

function expNameLinkRenderer(params) {
  var eDiv = document.createElement('div');
  eDiv.innerHTML = params.value;
  eDiv.classList.add("view-link");
  return eDiv;
}


export const EXP_COLUMN_DEF = [
  {
    headerName: "Last Updated Date",
    field: "experimentLastUpdatedTime",
    valueFormatter: timeFormatter,
    width: 100,
  },
  {
    headerName: "Experiment Name",
    field: "experimentName",
    cellRenderer: expNameLinkRenderer,
    width: 180,
    tooltipField: "experimentName",
  },
  {
    headerName: "Dataset Name",
    field: "datasetName",
    width: 150,
    tooltipField: "experimentName",
  },
  {
    headerName: "Prediction Column",
    field: "predictionField",
    width: 100,
    tooltipField: "experimentName",
  },

  {
    headerName: "Category",
    field: "category",
    width: 100,
    tooltipField: "experimentName",
  },
  {
    headerName: "Framework",
    field: "framework",
    width: 100,
    tooltipField: "experimentName",
  },
  {
    headerName: "# Models",
    field: "noOfModels",
    width: 80,
  },
  {
    headerName: "Author",
    field: "",
    cellRenderer: authorRenderer,
    width: 80,
  },
];
