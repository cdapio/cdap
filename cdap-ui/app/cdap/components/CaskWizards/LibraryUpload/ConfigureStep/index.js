/*
 * Copyright © 2017 Cask Data, Inc.
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
import { connect, Provider } from 'react-redux';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import { Col, Label, FormGroup, Form, Input } from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import T from 'i18n-react';

const mapStateToArtifactNameProps = (state) => {
  return {
    value: state.configure.name,
    type: 'text',
    placeholder: T.translate('features.Wizard.LibraryUpload.Step2.namePlaceholder'),
  };
};
const mapStateToArtifactDescriptionProps = (state) => {
  return {
    value: state.configure.description,
    type: 'textarea',
    rows: '7',
    placeholder: T.translate('features.Wizard.LibraryUpload.Step2.descriptionPlaceholder'),
  };
};
const mapStateToArtifactClassnameProps = (state) => {
  return {
    value: state.configure.classname,
    type: 'text',
    placeholder: T.translate('features.Wizard.LibraryUpload.Step2.classnamePlaceholder'),
  };
};
const mapStateToArtifactTypeInputProps = (state) => {
  return {
    value: state.configure.type,
    disabled: 'disabled',
    placeholder: T.translate('features.Wizard.LibraryUpload.Step2.typePlaceholder'),
  };
};
const mapStateToArtifactVersionProps = (state) => {
  return {
    value: state.configure.version,
    type: 'text',
    placeholder: T.translate('features.Wizard.LibraryUpload.Step2.versionPlaceholder'),
  };
};

const mapDispatchToArtifactNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: ArtifactUploadActions.setName,
        payload: { name: e.target.value },
      });
    },
  };
};
const mapDispatchToArtifactDescriptionProps = (dispatch) => {
  return {
    onChange: (e) =>
      dispatch({
        type: ArtifactUploadActions.setDescription,
        payload: { description: e.target.value },
      }),
  };
};
const mapDispatchToArtifactClassnameProps = (dispatch) => {
  return {
    onChange: (e) =>
      dispatch({
        type: ArtifactUploadActions.setClassname,
        payload: { classname: e.target.value },
      }),
  };
};

const mapDispatchToArtifactTypeProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: ArtifactUploadActions.setType,
        payload: {
          type: e.target.value,
        },
      });
    },
  };
};

const mapDispatchToArtifactVersionProps = (dispatch) => {
  return {
    onChange: (e) =>
      dispatch({
        type: ArtifactUploadActions.setVersion,
        payload: { version: e.target.value },
      }),
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
const TypeInput = connect(mapStateToArtifactTypeInputProps, mapDispatchToArtifactTypeProps)(Input);
const InputArtifactVersion = connect(
  mapStateToArtifactVersionProps,
  mapDispatchToArtifactVersionProps
)(InputWithValidations);

export default function ConfigureStep() {
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
            <Label className="control-label">
              {T.translate('features.Wizard.LibraryUpload.Step2.nameLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <InputArtifactName />
          </Col>
          <i className="fa fa-asterisk text-danger float-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.LibraryUpload.Step2.typeLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <TypeInput />
          </Col>
          <i className="fa fa-asterisk text-danger float-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.LibraryUpload.Step2.classnameLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <InputArtifactClassname />
          </Col>
          <i className="fa fa-asterisk text-danger float-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.LibraryUpload.Step2.versionLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <InputArtifactVersion />
          </Col>
          <i className="fa fa-asterisk text-danger float-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.LibraryUpload.Step2.descriptionLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <InputArtifactDescription />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}
