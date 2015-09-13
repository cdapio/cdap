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

angular.module(PKG.name + '.feature.apps')
  .controller('AppDetailProgramsController', function($state, $scope, $stateParams, MyDataSource, rAppData) {

    var basePath = '/apps/' + $stateParams.appId;
    this.programs = [];
    var datasrc = new MyDataSource($scope);

    rAppData.programs.forEach(function(prog) {
      prog.type_plural = prog.type +
        ((['Mapreduce', 'Spark'].indexOf(prog.type) === -1) ? 's': '');

      fetchStatus.bind(this)(prog.type_plural.toLowerCase(), prog.name);
    }.bind(this));

    this.programs = rAppData.programs;

    // FIXME: Not DRY. Almost same thing done in ProgramsListController
    function fetchStatus(program, programId) {
      datasrc.poll(
        {
          _cdapNsPath: basePath + '/' + program + '/' +
                        programId + '/status'
        },
        function (res) {
          var program = this.programs.filter(function(item) {
            return item.name === programId;
          }.bind(this));
          program[0].status = res.status;
        }.bind(this)
      );
    }

    this.goToDetail = function(programType, program) {
      $state.go(programType.toLowerCase() + '.detail', {
        programId: program
      });
    };

    //ui-sref="programs.type({programType: (program.type_plural | lowercase)})"
    this.goToList = function(programType) {
      $state.go(programType.toLowerCase() + '.list');
    };

  });
