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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * tests metrics table.
 */
public abstract class NonTxAwareTableTest extends TableTestBase {
  private static final byte ONES = 0x7f;

  protected abstract Table getTable(String name) throws Exception;

  protected static final byte[] A = Bytes.toBytes(1L);
  protected static final byte[] B = Bytes.toBytes(2L);
  protected static final byte[] C = Bytes.toBytes(3L);
  protected static final byte[] P = Bytes.toBytes(4L);
  protected static final byte[] Q = Bytes.toBytes(5L);
  protected static final byte[] R = Bytes.toBytes(6L);
  protected static final byte[] X = Bytes.toBytes(7L);
  protected static final byte[] Y = Bytes.toBytes(8L);
  protected static final byte[] Z = Bytes.toBytes(9L);

  @Test
  public void testGetPutSwap() throws Exception {
    Table table = getTable("testGetPutSwap");
    // put two rows
    table.put(ImmutableList.of(
      new Put(A).add(P, X).add(Q, Y),
      new Put(B).add(P, X).add(R, Z)));
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(A, P)));
    Assert.assertEquals(Bytes.toLong(Y), Bytes.toLong(table.get(A, Q)));
    Assert.assertNull(table.get(A, R));
    Assert.assertEquals(Bytes.toLong(X), Bytes.toLong(table.get(B, P)));
    Assert.assertEquals(Bytes.toLong(Z), Bytes.toLong(table.get(B, R)));
    Assert.assertNull(table.get(B, Q));

    // put one row again, with overlapping key set
    table.put(new Put(A).add(P, A).add(R, C));
    Assert.assertEquals(Bytes.toLong(A), Bytes.toLong(table.get(A, P)));
    Assert.assertEquals(Bytes.toLong(Y), Bytes.toLong(table.get(A, Q)));
    Assert.assertEquals(Bytes.toLong(C), Bytes.toLong(table.get(A, R)));

    // compare and swap an existing value, successfully
    Assert.assertTrue(table.compareAndSwap(A, P, A, B));
    Assert.assertEquals(Bytes.toLong(B), Bytes.toLong(table.get(A, P)));
    // compare and swap an existing value that does not match
    Assert.assertFalse(table.compareAndSwap(A, P, A, B));
    Assert.assertArrayEquals(B, table.get(A, P));
    // compare and swap an existing value that does not exist
    Assert.assertFalse(table.compareAndSwap(B, Q, A, B));
    Assert.assertNull(table.get(B, Q));

    // compare and delete an existing value, successfully
    Assert.assertTrue(table.compareAndSwap(A, P, B, null));
    Assert.assertNull(table.get(A, P));
    // compare and delete an existing value that does not match
    Assert.assertFalse(table.compareAndSwap(A, Q, A, null));
    Assert.assertArrayEquals(Y, table.get(A, Q));

    // compare and swap a null value, successfully
    Assert.assertTrue(table.compareAndSwap(A, P, null, Z));
    Assert.assertArrayEquals(Z, table.get(A, P));
    // compare and swap a null value, successfully
    Assert.assertFalse(table.compareAndSwap(A, Q, null, Z));
    Assert.assertArrayEquals(Y, table.get(A, Q));
  }

  protected class IncThread extends Thread implements Closeable {
    final Table table;
    final Increment increment;
    int rounds;

    public IncThread(Table table, Increment increment, int rounds) {
      this.table = table;
      this.increment = increment;
      this.rounds = rounds;
    }

    public void run() {
      while (rounds-- > 0) {
        try {
          table.increment(increment);
        } catch (Exception e) {
          System.err.println("exception for increment #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }

    @Override
    public void close() throws IOException {
      table.close();
    }
  }

  protected class IncAndGetThread extends Thread implements Closeable {
    final Table table;
    final byte[] row;
    final byte[] col;
    final long delta;
    int rounds;
    Long previous = 0L;

    public IncAndGetThread(Table table, byte[] row, byte[] col, long delta, int rounds) {
      this.table = table;
      this.row = row;
      this.col = col;
      this.delta = delta;
      this.rounds = rounds;
    }

    public void run() {
      while (rounds-- > 0) {
        try {
          long value = table.incrementAndGet(row, col, delta);
          Assert.assertTrue(value > previous);
        } catch (Exception e) {
          System.err.println("exception for increment and get #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }

    @Override
    public void close() throws IOException {
      table.close();
    }
  }

  @Test
  public void testConcurrentIncrement() throws Exception {
    final Table table = getTable("testConcurrentIncrement");
    final int rounds = 500;
    Increment inc1 = new Increment(A).add(X, 1L).add(Y, 2L);
    Increment inc2 = new Increment(A).add(Y, 1L).add(Z, 2L);
    Collection<? extends Thread> threads = ImmutableList.of(new IncThread(table, inc1, rounds),
                                                  new IncThread(table, inc2, rounds),
                                                  new IncAndGetThread(table, A, Z, 5, rounds),
                                                  new IncAndGetThread(table, A, Z, 2, rounds));
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
      if (t instanceof Closeable) {
        ((Closeable) t).close();
      }
    }

    Assert.assertEquals(rounds + 10L, table.incrementAndGet(A, X, 10L));
    Assert.assertEquals(3 * rounds - 20L, table.incrementAndGet(A, Y, -20L));
    Assert.assertEquals(9 * rounds, table.incrementAndGet(A, Z, 0L));
  }

  class SwapThread extends Thread {
    private final Table table;
    private final byte[] row;
    private final byte[] col;
    private final AtomicInteger[] counts;
    private final long rounds;

    SwapThread(Table table, byte[] row, byte[] col, AtomicInteger[] counts, long rounds) {
      this.table = table;
      this.row = row;
      this.col = col;
      this.counts = counts;
      this.rounds = rounds;
    }

    public void run() {
      for (long i = 0; i < rounds; i++) {
        try {
          boolean success = table.compareAndSwap(row, col, Bytes.toBytes(i), Bytes.toBytes(i + 1));
          counts[success ? 0 : 1].incrementAndGet();
        } catch (Exception e) {
          System.err.println("exception for swap #" + rounds + ": " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testConcurrentSwap() throws Exception {
    Table table = getTable("testConcurrentSwap");
    final long rounds = 500;
    table.put(new Put(A, B, 0L));
    AtomicInteger[] counts = { new AtomicInteger(), new AtomicInteger() }; // [0] for success, [1] for failures
    Thread t1 = new SwapThread(table, A, B, counts, rounds);
    Thread t2 = new SwapThread(table, A, B, counts, rounds);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    Assert.assertEquals(rounds, Bytes.toLong(table.get(A, B)));
    Assert.assertEquals(rounds, counts[0].get()); // number of successful swaps
    Assert.assertEquals(rounds, counts[1].get()); // number of failed swaps
  }

  @Test
  public void testDelete() throws Exception {
    Table table = getTable("testDelete");
    List<Put> puts = Lists.newArrayList();
    for (int i = 0; i < 1024; i++) {
      puts.add(new Put(Bytes.toBytes(i << 22), A, Bytes.toLong(X)));
    }
    table.put(puts);
    // verify the first and last are there (sanity test for correctness of test logic)
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0x00000000), A));
    Assert.assertArrayEquals(X, table.get(Bytes.toBytes(0xffc00000), A));

    List<Delete> toDelete = ImmutableList.of(
      new Delete(Bytes.toBytes(0xa1000000)),
      new Delete(Bytes.toBytes(0xb2000000)),
      new Delete(Bytes.toBytes(0xc3000000)));
    // verify these three are there, we will soon delete them
    for (Delete delete : toDelete) {
      Assert.assertArrayEquals(X, table.get(delete.getRow(), A));
    }
    // delete these three
    table.delete(toDelete);
    // verify these three are now gone.
    for (Delete delete : toDelete) {
      Assert.assertNull(table.get(delete.getRow(), A));
    }
    // verify nothing else is gone by counting all entries in a scan
    Assert.assertEquals(1021, countRange(table, null, null));

    // delete a range by prefix
    table.deleteAll(new byte[] { 0x11 });
    // verify that they are gone
    Assert.assertEquals(0, countRange(table, 0x11000000, 0x12000000));
    // verify nothing else is gone by counting all entries in a scan (deleted 4)
    Assert.assertEquals(1017, countRange(table, null, null));

    // delete all with prefix 0xff (it a border case because it has no upper bound)
    table.deleteAll(new byte[] { (byte) 0xff });
    // verify that they are gone
    Assert.assertEquals(0, countRange(table, 0xff000000, null));
    // verify nothing else is gone by counting all entries in a scan (deleted another 4)
    Assert.assertEquals(1013, countRange(table, null, null));

    // delete with empty prefix, should clear table
    // delete all with prefix 0xff (it a border case because it has no upper bound)
    table.deleteAll(new byte[0]);
    // verify everything is gone by counting all entries in a scan
    Assert.assertEquals(0, countRange(table, null, null));
  }

  @Test
  public void testDeleteIncrements() throws Exception {
    // note: this is pretty important test case for tables with counters, e.g. metrics
    Table table = getTable("testDeleteIncrements");
    // delete increment and increment again
    table.increment(A, ImmutableMap.of(B, 5L));
    table.increment(A, ImmutableMap.of(B, 2L));
    table.increment(P, ImmutableMap.of(Q, 15L));
    table.increment(P, ImmutableMap.of(Q, 12L));
    Assert.assertEquals(7L, Bytes.toLong(table.get(A, B)));
    Assert.assertEquals(27L, Bytes.toLong(table.get(P, Q)));
    table.delete(A, new byte[][]{B});
    table.deleteAll(P);
    Assert.assertNull(table.get(A, B));
    Assert.assertNull(table.get(P, Q));
    table.increment(A, ImmutableMap.of(B, 3L));
    table.increment(P, ImmutableMap.of(Q, 13L));
    Assert.assertEquals(3L, Bytes.toLong(table.get(A, B)));
    Assert.assertEquals(13L, Bytes.toLong(table.get(P, Q)));
  }

  private static int countRange(Table table, Integer start, Integer stop) throws Exception {
    Scanner scanner = table.scan(start == null ? null : Bytes.toBytes(start),
                                 stop == null ? null : Bytes.toBytes(stop), null, null);
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    return count;
  }

  @Test
  public void testFuzzyScan() throws Exception {
    Table table = getTable("testFuzzyScan");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    // now do a fuzzy scan of the table
    FuzzyRowFilter filter = new FuzzyRowFilter(
      ImmutableList.of(ImmutablePair.of(new byte[] { '*', 'b', '*', 'b' }, new byte[] { 0x01, 0x00, 0x01, 0x00 })));
    Scanner scanner = table.scan(null, null, new byte[][] { A }, filter);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      Assert.assertTrue(entry.getRow()[1] == 'b' && entry.getRow()[3] == 'b');
      Assert.assertEquals(1, entry.getColumns().size());
      Assert.assertTrue(entry.getColumns().containsKey(A));
      Assert.assertFalse(entry.getColumns().containsKey(B));
      count++;
    }
    Assert.assertEquals(9, count);
  }

  @Test
  public void testRangeDeleteWithoutFilter() throws Exception {
    Table table = getTable("rangeDeleteWithoutFilter");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    byte[] start = new byte[] { 'a', 0x00, 0x00, 0x00 };
    byte[] end = new byte[] { 'a', ONES, ONES, ONES };
    FuzzyRowFilter filter = new FuzzyRowFilter(
      ImmutableList.of(ImmutablePair.of(new byte[] { '*', 'b', '*', 'b' }, new byte[] { 0x01, 0x00, 0x01, 0x00 })));
    table.deleteRange(start, end, new byte[][] { A }, filter);

    // now do a scan of the table, making sure the cells with row like {a,b,*,b} and column A are gone.
    Scanner scanner = table.scan(null, null, null, null);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      byte[] row = entry.getRow();
      if (row[0] == 'a' && row[1] == 'b' && row[3] == 'b') {
        Assert.assertFalse(entry.getColumns().containsKey(A));
        Assert.assertEquals(1, entry.getColumns().size());
      } else {
        Assert.assertTrue(entry.getColumns().containsKey(A));
        Assert.assertTrue(entry.getColumns().containsKey(B));
        Assert.assertEquals(2, entry.getColumns().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

  @Test
  public void testRangeDeleteWithFilter() throws Exception {
    Table table = getTable("rangeDelete");
    NavigableMap<byte[], NavigableMap<byte[], Long>> writes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    byte[] abc = { 'a', 'b', 'c' };
    for (byte b1 : abc) {
      for (byte b2 : abc) {
        for (byte b3 : abc) {
          for (byte b4 : abc) {
            // we put two columns, but will scan only one column
            writes.put(new byte[] { b1, b2, b3, b4 }, Bytes.immutableSortedMapOf(A, Bytes.toLong(X), B,
                                                                                 Bytes.toLong(Y)));
          }
        }
      }
    }
    table.put(writes);
    // we should have 81 (3^4) rows now
    Assert.assertEquals(81, countRange(table, null, null));

    byte[] start = new byte[] { 'a', 0x00, 0x00, 0x00 };
    byte[] end = new byte[] { 'a', ONES, ONES, ONES };
    table.deleteRange(start, end, new byte[][] { A }, null);

    // now do a scan of the table, making sure the cells with row starting with 'a' and column A are gone.
    Scanner scanner = table.scan(null, null, null, null);
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      if (entry.getRow()[0] == 'a') {
        Assert.assertFalse(entry.getColumns().containsKey(A));
        Assert.assertEquals(1, entry.getColumns().size());
      } else {
        Assert.assertTrue(entry.getColumns().containsKey(A));
        Assert.assertTrue(entry.getColumns().containsKey(B));
        Assert.assertEquals(2, entry.getColumns().size());
      }
      count++;
    }
    Assert.assertEquals(81, count);
  }

}
