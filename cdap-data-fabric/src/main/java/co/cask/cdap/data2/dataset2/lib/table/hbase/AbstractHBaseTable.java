/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.dataset.table.EmptyRow;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.AbstractTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * An HBase metrics table client.
 */
// todo: verify/validate params
public class AbstractHBaseTable extends AbstractTable implements MeteredDataset {
  protected final HTableInterface hTable;
  private final String hTableName;
  private final byte[] columnFamily;
  private final boolean enableReadlessIncrements;

  // todo: emit metrics
  private MetricsCollector metricsCollector;

  public AbstractHBaseTable(DatasetSpecification spec,
                            HTableInterface hTable) throws IOException {
    this.hTable = hTable;
    this.hTableName = Bytes.toStringBinary(hTable.getTableName());
    this.columnFamily = HBaseTableAdmin.getColumnFamily(spec);
    this.enableReadlessIncrements = HBaseTableAdmin.supportsReadlessIncrements(spec);
  }

  @Override
  public void setMetricsCollector(MetricsCollector metricsCollector) {
    this.metricsCollector = metricsCollector;
  }

  @Override
  public Row get(byte[] row) {
    try {
      return getInternal(row, null);
    } catch (IOException e) {
      throw new DataSetException("Get failed on table " + hTableName, e);
    }
  }

  @Override
  public Row get(byte[] row, byte[][] columns) {
    try {
      return getInternal(row, columns);
    } catch (IOException e) {
      throw new DataSetException("Get columns failed on table " + hTableName, e);
    }
  }

  private Row getInternal(byte[] row, @Nullable byte[][] columns) throws IOException {
    // by contract Table sees its own writes immediately. So, we need to flush those to be included in results
    hTable.flushCommits();
    Get get = new Get(row);
    if (columns != null) {
      for (byte[] column : columns) {
        get.addColumn(columnFamily, column);
      }
    }
    Result getResult = hTable.get(get);
    if (!getResult.isEmpty()) {
      return new co.cask.cdap.api.dataset.table.Result(getResult.getRow(), getResult.getFamilyMap(columnFamily));
    }
    return EmptyRow.of(row);
  }

  @Nullable
  private byte[] getColumnInternal(byte[] row, byte[] column) throws IOException {
    // by contract Table sees its own writes immediately. So, we need to flush those to be included in results
    hTable.flushCommits();
    Get get = new Get(row);
    get.addColumn(columnFamily, column);
    Result getResult = hTable.get(get);
    if (!getResult.isEmpty()) {
      return getResult.getFamilyMap(columnFamily).get(column);
    }
    return null;
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    // todo: this is very inefficient: column range + limit should be pushed down via server-side filters
    try {
      Row rowResult = getInternal(row, null);
      NavigableMap<byte[], byte[]> rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      rowMap.putAll(rowResult.getColumns());
      return new co.cask.cdap.api.dataset.table.Result(row, getRange(rowMap, startColumn, stopColumn, limit));
    } catch (IOException e) {
      throw new DataSetException("Get column range failed on table " + hTableName, e);
    }
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    List<Put> puts = Lists.newArrayList();
    Put put = new Put(row);
    for (int i = 0; i < columns.length; i++) {
      put.add(columnFamily, columns[i], values[i]);
      puts.add(put);
    }
    try {
      hTable.put(puts);
    } catch (IOException e) {
      throw new DataSetException("Put failed on table " + hTableName, e);
    }
  }

  @Override
  public void delete(byte[] row) {
    Delete delete = new Delete(row);
    try {
      hTable.delete(delete);
    } catch (IOException e) {
      throw new DataSetException("Delete failed on table " + hTableName, e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    Delete delete = new Delete(row);
    for (byte[] column : columns) {
      delete.deleteColumns(columnFamily, column);
    }
    try {
      hTable.delete(delete);
    } catch (IOException e) {
      throw new DataSetException("Delete columns failed on table " + hTableName, e);
    }
  }

  @Override
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    // todo: this is very in-efficient, does extra roundtrip, due to unsupported increment by TransactionAwareHTable
    // Logic:
    // * fetching current values
    // * updating values
    // * returning updated values as result
    Row existing;
    try {
      existing = getInternal(row, columns);
    } catch (Exception e) {
      throw new DataSetException("IncrementAndGet failed on table " + hTableName, e);
    }

    Map<byte[], byte[]> rowMap = existing.getColumns();
    byte[][] updatedValues = new byte[columns.length][];
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < columns.length; i++) {
      byte[] column = columns[i];
      byte[] val = rowMap.get(column);
      // converting to long
      long longVal;
      if (val == null) {
        longVal = 0L;
      } else {
        if (val.length != Bytes.SIZEOF_LONG) {
          throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                            " row: " + Bytes.toStringBinary(row) +
                                            " column: " + Bytes.toStringBinary(column));
        }
        longVal = Bytes.toLong(val);
      }
      longVal += amounts[i];
      updatedValues[i] = Bytes.toBytes(longVal);
      result.put(column, updatedValues[i]);
    }

    put(row, columns, updatedValues);

    return new co.cask.cdap.api.dataset.table.Result(row, result);
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    if (!enableReadlessIncrements) {
      incrementAndGet(row, columns, amounts);
      return;
    }

    Put increment = getIncrementalPut(row, columns, amounts);
    try {
      hTable.put(increment);
    } catch (IOException e) {
      // figure out whether this is an illegal increment
      // currently there is not other way to extract that from the HBase exception than string match
      if (e.getMessage() != null && e.getMessage().contains("isn't 64 bits wide")) {
        throw new NumberFormatException("Attempted to increment a value that is not convertible to long," +
                                          " row: " + Bytes.toStringBinary(row));
      }
      try {
        throw e;
      } catch (IOException e1) {
        throw new DataSetException("Increment failed on table " + hTableName, e);
      }
    }
  }

  @Override
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) {
    // by contract Table sees its own writes immediately. So, we need to flush those to be included in results
    try {
      hTable.flushCommits();
    } catch (IOException e) {
      throw new DataSetException("Scan failed on table " + hTableName, e);
    }
    Scan scan = new Scan();
    scan.addFamily(columnFamily);
    // todo: should be configurable
    // NOTE: by default we assume scanner is used in mapreduce job, hence no cache blocks
    scan.setCacheBlocks(false);
    scan.setCaching(1000);

    if (startRow != null) {
      scan.setStartRow(startRow);
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow);
    }

    ResultScanner resultScanner = null;
    try {
      resultScanner = hTable.getScanner(scan);
    } catch (IOException e) {
      throw new DataSetException("Scan failed on table " + hTableName, e);
    }
    return new HBaseScanner(resultScanner, columnFamily);
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    byte[] existing;
    try {
      existing = getColumnInternal(row, column);
    } catch (IOException e) {
      throw new DataSetException("CompareAndSwap failed on table " + hTableName, e);
    }

    boolean existingAsExpected = (oldValue == null && existing == null) ||
                                 ((oldValue != null && existing != null) && Bytes.compareTo(oldValue, existing) == 0);
    if (!existingAsExpected) {
      return false;
    }

    if (newValue == null) {
      Delete delete = new Delete(row);
      // HBase API weirdness: we must use deleteColumns() because deleteColumn() deletes only the last version.
      delete.deleteColumns(columnFamily, column);
      try {
        hTable.delete(delete);
      } catch (IOException e) {
        throw new DataSetException("CompareAndSwap failed on table " + hTableName, e);
      }
    } else {
      Put put = new Put(row);
      put.add(columnFamily, column, newValue);
      try {
        hTable.put(put);
      } catch (IOException e) {
        throw new DataSetException("CompareAndSwap failed on table " + hTableName, e);
      }
    }

    return true;
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  private Put getIncrementalPut(byte[] row, byte[][] columns, long[] amounts) {
    Put increment = getIncrementalPut(row);
    for (int i = 0; i < columns.length; i++) {
      increment.add(columnFamily, columns[i], Bytes.toBytes(amounts[i]));
    }
    return increment;
  }

  private Put getIncrementalPut(byte[] row) {
    Put put = new Put(row);
    put.setAttribute(HBaseTable.DELTA_WRITE, Bytes.toBytes(true));
    return put;
  }

  private static <T> NavigableMap<byte[], T> getRange(NavigableMap<byte[], T> rowMap,
                                                      byte[] startColumn, byte[] stopColumn, int limit) {
    NavigableMap<byte[], T> result;
    if (startColumn == null && stopColumn == null) {
      result = rowMap;
    } else if (startColumn == null) {
      result = rowMap.headMap(stopColumn, false);
    } else if (stopColumn == null) {
      result = rowMap.tailMap(startColumn, true);
    } else {
      result = rowMap.subMap(startColumn, true, stopColumn, false);
    }
    return head(result, limit);
  }

  private static <T> NavigableMap<byte[], T> head(NavigableMap<byte[], T> map, int count) {
    if (count > 0 && map.size() > count) {
      // todo: is there better way to do it?
      byte [] lastToInclude = null;
      int i = 0;
      for (Map.Entry<byte[], T> entry : map.entrySet()) {
        lastToInclude = entry.getKey();
        if (++i >= count) {
          break;
        }
      }
      map = map.headMap(lastToInclude, true);
    }

    return map;
  }
}
