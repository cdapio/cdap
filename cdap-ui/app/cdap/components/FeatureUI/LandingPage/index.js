import React from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import AddFeatureWizard from '../AddFeatureWizard';
import FeatureTable from '../FeatureTable';
import {
  PIPELINE_TYPES,
  PIPELINES_REQUEST,
  PIPELINES_REQUEST_PARAMS,
  SERVER_IP,
  SCHEMA_REQUEST,
  PROPERTY_REQUEST,
  CONFIGURATION_REQUEST,
  GET_PIPELINE,
  SAVE_PIPELINE,
  GET_SCHEMA,
  GET_PROPERTY,
  GET_CONFIGURATION,
  IS_OFFLINE,
  SAVE_REQUEST,
  DELETE_REQUEST,
  DELETE_PIPELINE
} from '../config';
import { Observable } from 'rxjs/Observable';
import AlertModal from '../AlertModal';


require('./LandingPage.scss');

const SchemaData = [{ "schemaName": "accounts", "schemaColumns": [{ "columnName": "account_id", "columnType": "long" }] }, { "schemaName": "errors", "schemaColumns": [{ "columnName": "error_id", "columnType": "long" }, { "columnName": "account_id", "columnType": "long" }, { "columnName": "device_mac_address", "columnType": "string" }, { "columnName": "event_is_startup", "columnType": "boolean" }, { "columnName": "event_is_visible", "columnType": "boolean" }, { "columnName": "device_video_firmware", "columnType": "string" }, { "columnName": "event_cause_category", "columnType": "string" }, { "columnName": "event_cause_id", "columnType": "string" }, { "columnName": "hour", "columnType": "int" }, { "columnName": "device_video_model", "columnType": "string" }, { "columnName": "event_hostname", "columnType": "string" }, { "columnName": "event_date_time", "columnType": "string" }, { "columnName": "ets_timestamp", "columnType": "string" }, { "columnName": "date_hour", "columnType": "string" }, { "columnName": "count", "columnType": "int" }] }];
const PropertyData = [{ "paramName": "Indexes", "description": "" }, { "paramName": "Relationships", "description": "" }, { "paramName": "TimestampColumns", "description": "" }, { "paramName": "TimeIndexColumns", "description": "" }, { "paramName": "CategoricalColumns", "description": "" }, { "paramName": "IgnoreColumns", "description": "" }, { "paramName": "multiFieldTransFunctionInputColumns", "description": "" }, { "paramName": "multiFieldAggFunctionInputColumns", "description": "" }, { "paramName": "TargetEntity", "description": "" }, { "paramName": "TargetEntityPrimaryField", "description": "" }];
const ConfigurationData = [{ "paramName": "DFSDepth", "description": "", "isCollection": false, "dataType": "int" }, { "paramName": "TrainingWindows", "description": "", "isCollection": true, "dataType": "int" }, { "paramName": "WindowEndTime", "description": "", "isCollection": false, "dataType": "string" }];

class LandingPage extends React.Component {
  currentPipeline;
  constructor(props) {
    super(props);
    this.toggleFeatureWizard = this.toggleFeatureWizard.bind(this);
    this.onWizardClose = this.onWizardClose.bind(this);
    this.state = {
      data: [],
      dropdownOpen: false,
      showFeatureWizard: false,
      openAlertModal: false,
      alertMessage: "",
      pipelineTypes: PIPELINE_TYPES,
      selectedPipelineType: 'All',
    }
  }
  componentWillMount() {
    this.getPipelines(this.state.selectedPipelineType);
  }
  runOffline() {
    this.props.setAvailableProperties(PropertyData);
    this.props.setAvailableConfigurations(ConfigurationData);
    this.initWizard(SchemaData);
  }

  fetchWizardData() {
    this.fetchProperties();
    this.fetchConfiguration();
    this.fetchSchemas();
  }

  toggleFeatureWizard() {
    let open = !this.state.showFeatureWizard;
    if (open) {
      this.props.resetStore();
      if (IS_OFFLINE) {
        this.runOffline();
      } else {
        this.fetchWizardData();
      }
    } else {
      this.setState({
        showFeatureWizard: open
      });
    }
  }

  toggleDropDown() {
    this.setState(prevState => ({ dropdownOpen: !prevState.dropdownOpen }));
  }

  onPipeLineTypeChange(type) {
    this.setState({
      selectedPipelineType: type
    });
    this.getPipelines(type);
  }

  getPipelines(type) {
    let request = SERVER_IP + PIPELINES_REQUEST;
    if (type != "All") {
      request = request + PIPELINES_REQUEST_PARAMS + '=' + type;
    }
    fetch(request)
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["pipelineInfoList"])) {
            alert("Pipeline Data Error");
          } else {
            this.setState({
              data: result["pipelineInfoList"]
            });
          }
        },
        (error) => {
          this.handleError(error, GET_PIPELINE);
        }
      )
  }

  viewPipeline(pipeline) {
    this.currentPipeline = pipeline;
  }

  editPipeline(pipeline) {
    this.currentPipeline = pipeline;
  }

  onDeletePipeline(pipeline) {
    this.currentPipeline = pipeline;
    this.setState({
      openAlertModal: true,
      alertMessage: 'Are you sure you want to delete: ' + pipeline.pipelineName,
    })
  }

  onAlertClose(action) {
    if (action === 'OK' && this.currentPipeline) {
      this.deletePipeline(this.currentPipeline);
    }
    this.setState({
      openAlertModal: false
    })
  }

  deletePipeline(pipeline) {
    let fetchUrl = SERVER_IP + DELETE_REQUEST.replace('$NAME', pipeline.pipelineName);
    fetch(fetchUrl, {
      method: 'DELETE',
    })
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || (result.status && result.status > 200)) {
            this.handleError(result, DELETE_PIPELINE);
          } else {
            this.getPipelines(this.state.selectedPipelineType);
          }
        },
        (error) => {
          this.handleError(error, DELETE_PIPELINE);
        }
      )
  }

  handleError(error, type) {
    error.message ? alert(error.message) : alert(error);
  }

  onWizardClose() {
    console.log('Toggle');
    this.setState({
      showFeatureWizard: !this.state.showFeatureWizard
    });
  }

  openConfirmationModal() {
    this.setState({
      openConfirmation: true
    });
  }

  closeConfirmationModal() {
    this.setState({
      openConfirmation: false
    })
  }


  saveFeature() {
    let featureObject = this.getFeatureObject(this.props);
    let saveUrl = SERVER_IP + SAVE_REQUEST.replace('$NAME', featureObject.pipelineRunName);
    console.log(featureObject);
    return Observable.create((observer) => {
      fetch(saveUrl, {
        method: 'POST',
        body: JSON.stringify(featureObject)
      }).then(res => res.json())
        .then(
          (result) => {
            if (isNil(result) || (result.status && result.status > 200)) {
              this.handleError(result, SAVE_PIPELINE);
              observer.error(result);
            } else {
              this.getPipelines(this.state.selectedPipelineType);
              observer.next(result);
              observer.complete();
            }
          },
          (error) => {
            this.handleError(error, SAVE_PIPELINE);
            observer.error(error);
          }
        )
    });
  }


  getFeatureObject(props) {
    let featureObject = {
      pipelineRunName: props.featureName
    };

    if (!isEmpty(props.selectedSchemas)) {
      featureObject["dataSchemaNames"] = props.selectedSchemas.map(schema => schema.schemaName);
    }
    if (!isNil(props.propertyMap)) {
      props.propertyMap.forEach((value, property) => {
        if (value) {
          featureObject[property] = [];
          let subPropObj = {};
          value.forEach(subParam => {
            if (subParam.header == "none") {
              subParam.value.forEach((columns, schema) => {
                if (!isEmpty(columns)) {
                  columns.forEach((column) => {
                    if (subParam.isCollection) {
                      featureObject[property].push({
                        table: schema,
                        column: column.columnName
                      });
                    } else {
                      featureObject[property] = {
                        table: schema,
                        column: column.columnName
                      }
                    }
                  });
                }
              });
            } else {
              subParam.value.forEach((columns, schema) => {
                if (!isEmpty(columns)) {
                  let subPropValue = subParam.isCollection ? [] : {};
                  columns.forEach((column) => {
                    if (subParam.isCollection) {
                      subPropValue.push({
                        table: schema,
                        column: column.columnName
                      });
                    } else {
                      subPropValue = {
                        table: schema,
                        column: column.columnName
                      }
                    }
                  });
                  subPropObj[subParam.header] = subPropValue;
                }
              });
            }
          })
          if(!isEmpty(subPropObj)) {
            featureObject[property].push(subPropObj);
          }
        }
      })
    }
    if (!isEmpty(props.configurationList)) {
      props.configurationList.forEach((configuration) => {
        if (!isEmpty(configuration.value)) {
          switch (configuration.dataType) {
            case 'int':
              if (configuration.isCollection) {
                let values = configuration.value.split(",");
                featureObject[configuration.name] = values.map(value => parseInt(value));
              } else {
                featureObject[configuration.name] = parseInt(configuration.value);
              }
              break;
            default:
              if (configuration.isCollection) {
                featureObject[configuration.name] = configuration.value.split(",");
              } else {
                featureObject[configuration.name] = configuration.value;
              }
          }
        }
      });
    }
    return featureObject;
  }



  initWizard(data) {
    this.props.setAvailableSchemas(data);
    this.setState({
      showFeatureWizard: true
    });
  }

  fetchSchemas() {
    let fetchUrl = SERVER_IP + SCHEMA_REQUEST;
    fetch(fetchUrl)
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["dataSchemaList"])) {
            this.handleError(result, GET_SCHEMA);
          } else {
            this.initWizard(result["dataSchemaList"]);
          }
        },
        (error) => {
          this.handleError(error, GET_SCHEMA);
        }
      )
  }

  fetchProperties() {
    let fetchUrl = SERVER_IP + PROPERTY_REQUEST;
    fetch(fetchUrl)
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["configParamList"])) {
            this.handleError(result, GET_PROPERTY);
          } else {
            this.props.setAvailableProperties(result["configParamList"]);
          }
        },
        (error) => {
          this.handleError(error, GET_PROPERTY);
        }
      )
  }

  fetchConfiguration() {
    let fetchUrl = SERVER_IP + CONFIGURATION_REQUEST;
    fetch(fetchUrl)
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["configParamList"])) {
            this.handleError(result, GET_CONFIGURATION);
          } else {
            this.props.setAvailableConfigurations(result["configParamList"]);
          }
        },
        (error) => {
          this.handleError(error, GET_CONFIGURATION);
        }
      )
  }
  render() {
    return (
      <div className='landing-page-container'>
        <div className='top-control'>
          <Dropdown isOpen={this.state.dropdownOpen} toggle={this.toggleDropDown.bind(this)}>
            <DropdownToggle caret>
              {this.state.selectedPipelineType}
            </DropdownToggle>
            <DropdownMenu>
              {
                this.state.pipelineTypes.map((type) => {
                  return (
                    <DropdownItem onClick={this.onPipeLineTypeChange.bind(this, type)}>{type}</DropdownItem>
                  )
                })
              }
            </DropdownMenu>
          </Dropdown>
          <button className="feature-button" onClick={this.toggleFeatureWizard}>+ Add New</button>
        </div>
        <FeatureTable data={this.state.data}
          onView={this.viewPipeline.bind(this)}
          onEdit={this.editPipeline.bind(this)}
          onDelete={this.onDeletePipeline.bind(this)} />
        <AddFeatureWizard showWizard={this.state.showFeatureWizard}
          onClose={this.onWizardClose}
          onSubmit={this.saveFeature.bind(this)} />
        <AlertModal open={this.state.openAlertModal} message={this.state.alertMessage}
            onClose={this.onAlertClose.bind(this)} />
      </div>
    );
  }
}
export default LandingPage;