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
import React from 'react';
// import T from 'i18n-react';
// import {Col, FormGroup, Label, Form} from 'reactstrap';
// import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import NamespacePreferences from 'components/NamespacePreferences';

// import AddNamespaceActions  from 'services/WizardStores/AddNamespace/AddNamespaceActions';
// import InputWithValidations from 'components/InputWithValidations';

// import {Provider, connect} from 'react-redux';
//
// //Preference Name
// const mapStateToPreferenceNameProps = (state) => {
//   return {
//     value: state.preferences.preferencesKey,
//     type: 'text',
//     placeholder: T.translate('features.Wizard.Add-Namespace.Step4.name-placeholder')
//   };
// };
//
// const mapDispatchToPreferenceNameProps = (dispatch) => {
//   return {
//     onChange: (e) => {
//       dispatch({
//         type: AddNamespaceActions.setPreferencesKey,
//         payload: { preferencesKey : e.target.value }
//       });
//     }
//   };
// };
//
// //Preference Value
// const mapStateToPreferenceValueProps = (state) => {
//   return {
//     value: state.preferences.preferencesVal,
//     type: 'text',
//     placeholder: T.translate('features.Wizard.Add-Namespace.Step4.value-placeholder')
//   };
// };
//
// const mapDispatchToPreferenceValueProps = (dispatch) => {
//   return {
//     onChange: (e) => {
//       dispatch({
//         type: AddNamespaceActions.setPreferencesVal,
//         payload: { preferencesVal : e.target.value }
//       });
//     }
//   };
// };
//
// const InputPreferencesName = connect(
//   mapStateToPreferenceNameProps,
//   mapDispatchToPreferenceNameProps
// )(InputWithValidations);
//
// const InputPreferencesValue = connect(
//   mapStateToPreferenceValueProps,
//   mapDispatchToPreferenceValueProps
// )(InputWithValidations);

export default function PreferencesStep() {

  return (
    <div>
      <NamespacePreferences />
    </div>
  );

  // return(
  //     <Provider store={AddNamespaceStore}>
  //
  //     </Provider>
  // );
}
