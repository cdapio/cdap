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

angular.module(PKG.name + '.feature.tracker')
  .controller('TypeaheadTrackerTagsCtrl', function() {
    this.list = [ ];
    this.modelLoading = false;
    this.isOpen = true;
    this.template = '/assets/features/tracker/directives/typeahead-tracker-tags/popup.html';
    this.list = [
      {
        name: 'Tag1',
        count: 10,
      },
      {
        name: 'Tag2',
        count: 130
      },
      {
        name: 'Super Tag',
        count: 130
      },
      {
        name: 'Preferred Tag 1',
        count: 30,
        preferredTag: true
      },
      {
        name: 'Super Preferred Tag 3',
        count: 10,
        preferredTag: true
      },
      {
        name: 'Tag4',
        count: 0
      }
    ];
    this.list = this.list.map(function(tag) {
      return {
        name: tag.name,
        count: tag.count,
        label: tag.preferredTag ? (tag.name + ' (' + tag.count + ') ') : tag.name,
        preferredTag: (tag.preferredTag ? tag.count: -1)
      };
    });
    this.list.sort(function(a,b) {
      return a.preferredTag < b.preferredTag ? 1 : -1;
    });
    this.onTagsSelect = function(item) {
      this.model = item.name;
    };
  });
