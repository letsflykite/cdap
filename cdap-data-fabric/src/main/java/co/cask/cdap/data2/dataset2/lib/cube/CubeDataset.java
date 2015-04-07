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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.timeseries.EntityTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class CubeDataset implements Cube, Dataset {
  private final Map<Integer, Table> resolutionTables;
  private final Table entityTable;
  private final DefaultCube cube;

  public CubeDataset(String name, Table entityTable, Table table1, Table table60, Table table3600, Table tableTotals) {
    this.entityTable = entityTable;
    this.resolutionTables = ImmutableMap.of(
      1, table1,
      60, table1,
      3600, table1,
      Integer.MAX_VALUE, table1
    );
    this.cube = new DefaultCube(new int[] {1, 60, 3600, Integer.MAX_VALUE},
                                new FactTableSupplierImpl(resolutionTables),
                                );
  }

  @Override
  public void add(CubeFact fact) throws Exception {
    cube.add(fact);
  }

  @Override
  public void add(Collection<? extends CubeFact> facts) throws Exception {
    cube.add(facts);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) throws Exception {
    return cube.query(query);
  }

  @Override
  public void delete(CubeDeleteQuery query) throws Exception {
    cube.delete(query);
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(CubeExploreQuery query) throws Exception {
    return cube.findNextAvailableTags(query);
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery query) throws Exception {
    return cube.findMeasureNames(query);
  }

  @Override
  public void close() throws IOException {
    for (Table table : resolutionTables.values()) {
      table.close();
    }
  }

  private static final class FactTableSupplierImpl implements FactTableSupplier {
    private final Table entityTable;
    private final Map<Integer, Table> resolutionTables;

    private FactTableSupplierImpl(Table entityTable, Map<Integer, Table> resolutionTables) {
      this.entityTable = entityTable;
      this.resolutionTables = resolutionTables;
    }

    @Override
    public FactTable get(int resolution, int rollTime) {
      return new FactTable(new MetricsTableImpl(resolutionTables.get(resolution)),
                           new EntityTable(new MetricsTableImpl(entityTable)),
                           resolution, rollTime);
    }
  }
}
