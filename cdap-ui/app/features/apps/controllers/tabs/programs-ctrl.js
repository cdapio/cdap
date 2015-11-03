/*
 * Copyright © 2015 Cask Data, Inc.
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

class AppDetailProgramsController {
  constructor($state, $scope, $stateParams, MyCDAPDataSource, rAppData) {

    this.$state = $state;
    this.$stateParams = $stateParams;
    this.programs = [];
    this.datasrc = new MyCDAPDataSource($scope);

    rAppData.programs.forEach( (prog)=> {
      prog.type_plural = prog.type +
        ((['Mapreduce', 'Spark'].indexOf(prog.type) === -1) ? 's': '');

      this.fetchStatus(prog.type_plural.toLowerCase(), prog.name);
    });

    this.programs = rAppData.programs;
  }
  fetchStatus(program, programId) {
    var basePath = `/apps/${this.$stateParams.appId}`;
    this.datasrc.poll({ _cdapNsPath: `${basePath}/${program}/${programId}/status` },
      (res)=> {
        var program = this.programs.filter( (item) => { return item.name === programId; });
        program[0].status = res.status;
      }
    );
  }
  goToDetail(programType, program) {
    this.$state.go(`${programType.toLowerCase()}.detail`, {
      programId: program
    });
  }
  goToList(programType) {
    this.$state.go(`${programType.toLowerCase()}.list`);
  }
}

AppDetailProgramsController.$inject = ['$state', '$scope', '$stateParams', 'MyCDAPDataSource', 'rAppData'];

angular.module(PKG.name + '.feature.apps')
  .controller('AppDetailProgramsController', AppDetailProgramsController);
