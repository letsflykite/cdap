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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import org.junit.Assert;

import java.util.Map;

/**
 *
 */
public abstract class TableTestBase {
  protected static void verify(byte[][] expected, Row row) {
    verify(expected, row.getColumns());
  }

  protected static void verify(byte[][] expected, Map<byte[], byte[]> rowMap) {
    Assert.assertEquals(expected.length / 2, rowMap.size());
    for (int i = 0; i < expected.length; i += 2) {
      byte[] key = expected[i];
      byte[] val = expected[i + 1];
      Assert.assertArrayEquals(val, rowMap.get(key));
    }
  }

  protected static void verify(byte[][] expectedColumns, byte[][] expectedValues, Row row) {
    Map<byte[], byte[]> actual = row.getColumns();
    Assert.assertEquals(expectedColumns.length, actual.size());
    for (int i = 0; i < expectedColumns.length; i++) {
      Assert.assertArrayEquals(expectedValues[i], actual.get(expectedColumns[i]));
    }
  }

  protected static void verify(byte[] expected, byte[] actual) {
    Assert.assertArrayEquals(expected, actual);
  }

  protected static void verify(byte[][] expectedRows, byte[][][] expectedRowMaps, Scanner scan) {
    for (int i = 0; i < expectedRows.length; i++) {
      Row next = scan.next();
      Assert.assertNotNull(next);
      Assert.assertArrayEquals(expectedRows[i], next.getRow());
      verify(expectedRowMaps[i], next.getColumns());
    }

    // nothing is left in scan
    Assert.assertNull(scan.next());
  }

  protected static  long[] la(long... elems) {
    return elems;
  }

  protected static  byte[][] lb(long... elems) {
    byte[][] elemBytes = new byte[elems.length][];
    for (int i = 0; i < elems.length; i++) {
      elemBytes[i] = Bytes.toBytes(elems[i]);
    }
    return elemBytes;
  }

  // to array
  protected static  byte[][] a(byte[]... elems) {
    return elems;
  }

  // to array of array
  protected static  byte[][][] aa(byte[][]... elems) {
    return elems;
  }
}
