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
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TxConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class TxAwareHBaseTable extends AbstractHBaseTable implements TransactionAware {
  private final String transactionAwareName;
  private final TransactionAware txAwareTable;
  private final ConflictDetection conflictLevel;

  public TxAwareHBaseTable(DatasetContext datasetContext, DatasetSpecification spec,
                           CConfiguration cConf, Configuration hConf,
                           HBaseTableUtil tableUtil) throws IOException {
    super(spec, createHTable(datasetContext, spec, hConf, tableUtil));

    this.transactionAwareName = PrefixedNamespaces.namespace(cConf, datasetContext.getNamespaceId(), spec.getName());
    this.txAwareTable = (TransactionAware) hTable;
    this.conflictLevel = getConflictDetectionLevel(spec);
  }

  private static HTableInterface createHTable(DatasetContext datasetContext, DatasetSpecification spec,
                                              Configuration hConf, HBaseTableUtil tableUtil)
    throws IOException {
    ConflictDetection conflictLevel = getConflictDetectionLevel(spec);
    TxConstants.ConflictDetection level;
    switch (conflictLevel) {
      case ROW:
        level = TxConstants.ConflictDetection.ROW;
        break;
      case COLUMN:
        level = TxConstants.ConflictDetection.COLUMN;
        break;
      case NONE:
        // in case of NONE, we will ignore the changeset in getTxChanges() method
        level = TxConstants.ConflictDetection.ROW;
        break;
      default:
        throw new RuntimeException("Unknown conflict detection level: " + conflictLevel);
    }
    HTableInterface hTable =
      tableUtil.createTransactionAwareHTable(hConf,
                                             TableId.from(datasetContext.getNamespaceId(), spec.getName()),
                                             level);

    // todo: explain. move to abstract hbase table?
    hTable.setAutoFlush(false);

    return hTable;
  }

  private static ConflictDetection getConflictDetectionLevel(DatasetSpecification spec) {
    return ConflictDetection.valueOf(spec.getProperty(PROPERTY_CONFLICT_LEVEL, ConflictDetection.ROW.name()));
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    // todo: we have to override currently to use the one with read, as transactional puts override each other:
    //       they use same timestamp
    incrementAndGet(row, columns, amounts);
  }

  @Override
  public void startTx(Transaction transaction) {
    txAwareTable.startTx(transaction);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return ConflictDetection.NONE == conflictLevel ? Collections.<byte[]>emptyList() : txAwareTable.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return txAwareTable.commitTx();
  }

  @Override
  public void postTxCommit() {
    txAwareTable.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return txAwareTable.rollbackTx();
  }

  @Override
  public String getTransactionAwareName() {
    return transactionAwareName;
  }
}
