/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
import { connect, Provider } from 'react-redux';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import { Col, Label, FormGroup, Form, Input } from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import SelectWithOptions from 'components/SelectWithOptions';
import T from 'i18n-react';


const mapStateToArtifactNameProps = (state) => {
  return {
    value: state.configure.name,
    type: 'text',
    placeholder: T.translate('features.Wizard.ArtifactUpload.Step2.namePlaceholder')
  };
};
const mapStateToArtifactDescriptionProps = (state) => {
  return {
    value: state.configure.description,
    type: 'textarea',
    rows: '7',
    placeholder: T.translate('features.Wizard.ArtifactUpload.Step2.decriptionPlaceholder')
  };
};
const mapStateToArtifactClassnameProps = (state) => {
  return {
    value: state.configure.classname,
    type: 'text',
    placeholder: T.translate('features.Wizard.ArtifactUpload.Step2.classnamePlaceholder')
  };
};
const mapStateToArtifactTypeSelectProps = (state) => {
  return {
    options: [{id: 'jdbc', value: 'jdbc'}],
    value: state.configure.type
  };
};
const mapStateToArtifactTypeInputProps = (state) => {
  return {
    value: state.configure.type,
    placeholder: T.translate('features.Wizard.ArtifactUpload.Step2.typePlaceholder')
  };
};

const mapDispatchToArtifactNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: ArtifactUploadActions.setName,
        payload: {name: e.target.value}
      });
    }
  };
};
const mapDispatchToArtifactDescriptionProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: ArtifactUploadActions.setDescription,
      payload: {description: e.target.value}
    }))
  };
};
const mapDispatchToArtifactClassnameProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: ArtifactUploadActions.setClassname,
      payload: {classname: e.target.value}
    }))
  };
};

const mapDispatchToArtifactTypeProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: ArtifactUploadActions.setType,
        payload: {
          type: e.target.value
        }
      });
    }
  };
};

const InputArtifactName = connect(
  mapStateToArtifactNameProps,
  mapDispatchToArtifactNameProps
)(InputWithValidations);
const InputArtifactDescription = connect(
  mapStateToArtifactDescriptionProps,
  mapDispatchToArtifactDescriptionProps
)(InputWithValidations);
const InputArtifactClassname = connect(
  mapStateToArtifactClassnameProps,
  mapDispatchToArtifactClassnameProps
)(InputWithValidations);
const TypeSelect = connect(
  mapStateToArtifactTypeSelectProps,
  mapDispatchToArtifactTypeProps
)(SelectWithOptions);
const TypeInput = connect(
  mapStateToArtifactTypeInputProps,
  mapDispatchToArtifactTypeProps
)(Input);


export default function ConfigureStep({isMarket}) {
  let getTypeComponent = () => {
    if (isMarket) {
      return (<TypeSelect />);
    }
    return (<TypeInput />);
  };
  return (
    <Provider store={ArtifactUploadStore}>
      <Form
        className="form-horizontal general-info-step"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.ArtifactUpload.Step2.nameLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputArtifactName />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.ArtifactUpload.Step2.typeLabel')}</Label>
          </Col>
          <Col xs="7">
            {getTypeComponent()}
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.ArtifactUpload.Step2.classnameLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputArtifactClassname />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.ArtifactUpload.Step2.descriptionLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputArtifactDescription />
          </Col>
        </FormGroup>

      </Form>
    </Provider>
  );
}

ConfigureStep.propTypes = {
  isMarket: PropTypes.bool
};
