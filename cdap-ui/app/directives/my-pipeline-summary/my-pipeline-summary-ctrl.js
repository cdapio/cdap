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

class MyPipelineSummaryCtrl {
  constructor() {
    this.runs = [];
    this.programId = '';
    this.programType = '';
    this.appId = '';
    this.setState();
    this.store.registerOnChangeListener(this.setState.bind(this));
  }
  setState() {
    this.totalRunsCount = this.store.getRunsCount();
    this.runs = this.store.getRuns();
    var params = this.store.getParams();
    this.programType = params.programType.toUpperCase();
    this.programId = params.programName;
    this.appId = params.app;
  }
}

 angular.module(PKG.name + '.commons')
  .controller('MyPipelineSummaryCtrl', MyPipelineSummaryCtrl);
