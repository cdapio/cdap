/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import PaginationStepper from 'components/PaginationStepper';
import './Pagination.scss';

interface IPaginationProps {
  setPage: (page: number) => void;
  currentPage: number;
  numPipelines: number;
  pageLimit: number;
  shouldDisplay: boolean;
}

const PaginationView: React.SFC<IPaginationProps> = ({
  setPage,
  currentPage,
  numPipelines,
  pageLimit,
  shouldDisplay = true,
}) => {
  const numPages = Math.ceil(numPipelines / pageLimit);

  if (!shouldDisplay || numPages <= 1) {
    return null;
  }

  const prevDisabled = currentPage === 1;
  const nextDisabled = currentPage === numPages;

  function handleNext() {
    if (nextDisabled) {
      return;
    }

    setPage(currentPage + 1);
  }

  function handlePrev() {
    if (prevDisabled) {
      return;
    }

    setPage(currentPage - 1);
  }

  return (
    <div className="pagination-container float-right">
      <PaginationStepper
        onNext={handleNext}
        onPrev={handlePrev}
        nextDisabled={nextDisabled}
        prevDisabled={prevDisabled}
      />
    </div>
  );
};

export default PaginationView;
