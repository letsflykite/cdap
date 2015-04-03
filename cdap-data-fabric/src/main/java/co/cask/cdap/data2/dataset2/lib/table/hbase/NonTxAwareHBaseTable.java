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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 *
 */
public class NonTxAwareHBaseTable extends AbstractHBaseTable {

  public NonTxAwareHBaseTable(DatasetContext datasetContext, DatasetSpecification spec,
                              CConfiguration cConf, Configuration hConf,
                              HBaseTableUtil tableUtil) throws IOException {
    super(spec, createHTable(datasetContext, spec, hConf, tableUtil));
  }

  private static HTableInterface createHTable(DatasetContext datasetContext, DatasetSpecification spec,
                                              Configuration hConf, HBaseTableUtil tableUtil)
    throws IOException {
    HTableInterface hTable =
      tableUtil.createHTable(hConf, TableId.from(datasetContext.getNamespaceId(), spec.getName()));

    // todo: explain. move to abstract hbase table?
    hTable.setAutoFlush(false);

    return hTable;
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    // todo: we have to override currently to use the one with read, as transactional puts override each other:
    //       they use same timestamp
    incrementAndGet(row, columns, amounts);
  }
}
