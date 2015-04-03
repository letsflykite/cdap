/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public class TransactionalHBaseTableDatasetTest extends HBaseTableDatasetTestBase {
  @Override
  protected BufferingTable getTable(DatasetContext datasetContext, String name,
                                    ConflictDetection conflictLevel) throws Exception {
    // ttl=-1 means "keep data forever"
    DatasetSpecification spec = DatasetSpecification.builder(name, "foo")
      .property(Table.PROPERTY_READLESS_INCREMENT, "true")
      .property(Table.PROPERTY_CONFLICT_LEVEL, conflictLevel.name())
      .build();
    return new HBaseTable(datasetContext, spec, cConf, testHBase.getConfiguration(), hBaseTableUtil);
  }

  @Override
  protected HBaseTableAdmin getTableAdmin(DatasetContext datasetContext, String name,
                                          DatasetProperties props) throws IOException {
    DatasetSpecification spec = new HBaseTableDefinition("foo").configure(name, props);
    return new HBaseTableAdmin(datasetContext, spec, testHBase.getConfiguration(), hBaseTableUtil,
                               cConf, new LocalLocationFactory(tmpFolder.newFolder()));
  }

  @Test
  public void testEnableIncrements() throws Exception {
    // do nothing: not supported as of now: increment falls back to incrementAndGet currently
    // todo: do readless increments
  }
}
