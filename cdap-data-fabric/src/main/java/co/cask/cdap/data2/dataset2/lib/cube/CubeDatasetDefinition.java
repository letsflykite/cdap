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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CubeDatasetDefinition extends AbstractDatasetDefinition<CubeDataset, DatasetAdmin> {
  private final DatasetDefinition<? extends Table, ?> tableDef;

  /**
   * Creates instance of {@link CubeDatasetDefinition}.
   *
   * @param name this dataset type name
   */
  public CubeDatasetDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    // todo: test that you can specify different TTL for different resolution
    // Configuring tables that hold data of specific resolution
    List<DatasetSpecification> datasetSpecs = ImmutableList.of(
      tableDef.configure("1", properties),
      tableDef.configure("60", properties),
      tableDef.configure("3600", properties),
      tableDef.configure(String.valueOf(Integer.MAX_VALUE), properties)
    );

    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(datasetSpecs)
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(ImmutableList.of(
      tableDef.getAdmin(datasetContext, spec.getSpecification("1"), classLoader),
      tableDef.getAdmin(datasetContext, spec.getSpecification("60"), classLoader),
      tableDef.getAdmin(datasetContext, spec.getSpecification("3600"), classLoader),
      tableDef.getAdmin(datasetContext, spec.getSpecification(String.valueOf(Integer.MAX_VALUE)), classLoader)
    ));
  }

  @Override
  public CubeDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                 Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table1 = tableDef.getDataset(datasetContext, spec.getSpecification("1"), arguments, classLoader);
    Table table60 = tableDef.getDataset(datasetContext, spec.getSpecification("60"), arguments, classLoader);
    Table table3600 = tableDef.getDataset(datasetContext, spec.getSpecification("3600"), arguments, classLoader);
    Table tableTotals = tableDef.getDataset(datasetContext, spec.getSpecification(String.valueOf(Integer.MAX_VALUE)),
                                            arguments, classLoader);

    // todo: extract configured aggregations from spec

    return new CubeDataset(spec.getName(), table1, table60, table3600, tableTotals);
  }
}
