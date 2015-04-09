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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dataset definition of {@link CubeDataset}, the dataset that implements {@link co.cask.cdap.api.dataset.lib.cube.Cube}
 * to store and query {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}s.
 * <p/>
 * Cube dataset can be configured with aggregation resolutions and aggregations. E.g.
    <pre>
     dataset.cube.resolutions=1,60
     dataset.cube.aggregation.userPages.tags=user,page
     dataset.cube.aggregation.userPages.requiredTags=page
     dataset.cube.aggregation.userActions.tags=user,action
     dataset.cube.aggregation.userActions.requiredTags=action
    </pre>
 *
 * <ul>
 *   <li>
 *     configures Cube to aggregate data for 1 second and 60 seconds resolutions
 *   </li>
 *   <li>
 *     configures "userPages" aggregation (name doesn't have any restricted format, can be any alphabetical) that
 *     aggregates measurements for user and page; allows querying e.g. number of visits of specific user of specific
 *     page
 *   </li>
 *   <li>
 *     configures "userActions" aggregation (name doesn't have any restricted format, can be any alphabetical) that
 *     aggregates measurements for user and action; allows querying e.g. number of specific actions of specific user
 *   </li>
 * </ul>
 *
 * Aggregation is defined with list of tags to aggregate by and a list of required tags
 * (dataset.cube.aggregation.[agg_name].tags and dataset.cube.aggregation.[agg_name].requiredTags properties
 * respectively). The {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} measurement is aggregated within an aggregation
 * if it contains all required tags which non-null value.
 */
public class CubeDatasetDefinition extends AbstractDatasetDefinition<CubeDataset, DatasetAdmin> {
  public static final String PROPERTY_RESOLUTIONS = "dataset.cube.resolutions";
  public static final String PROPERTY_AGGREGATION_PREFIX = "dataset.cube.aggregation.";
  public static final String PROPERTY_TAGS = "tags";
  public static final String PROPERTY_REQUIRED_TAGS = "requiredTags";
  // 1 second is the only default resolution
  public static final int[] DEFAULT_RESOLUTIONS = new int[]{1};

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
    int[] resolutions = getResolutions(properties.getProperties());

    // Configuring tables that hold data of specific resolution
    List<DatasetSpecification> datasetSpecs = Lists.newArrayList();
    datasetSpecs.add(tableDef.configure("entity", properties));
    // NOTE: we create a table per resolution; we later will use that to e.g. configure ttl separatelly for each
    for (int resolution : resolutions) {
      datasetSpecs.add(tableDef.configure(String.valueOf(resolution), properties));
    }

    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(datasetSpecs)
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    List<DatasetAdmin> admins = Lists.newArrayList();

    admins.add(tableDef.getAdmin(datasetContext, spec.getSpecification("entity"), classLoader));

    int[] resolutions = getResolutions(spec.getProperties());
    for (int resolution : resolutions) {
      admins.add(tableDef.getAdmin(datasetContext, spec.getSpecification(String.valueOf(resolution)), classLoader));
    }

    return new CompositeDatasetAdmin(admins);
  }

  @Override
  public CubeDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    Table entityTable = tableDef.getDataset(datasetContext, spec.getSpecification("entity"), arguments, classLoader);

    int[] resolutions = getResolutions(spec.getProperties());
    Map<Integer, Table> resolutionTables = Maps.newHashMap();
    for (int resolution : resolutions) {
      resolutionTables.put(resolution,
                           tableDef.getDataset(datasetContext, spec.getSpecification(String.valueOf(resolution)),
                                               arguments, classLoader));
    }

    Collection<Aggregation> aggregations = getAggregations(spec.getProperties());

    return new CubeDataset(spec.getName(), entityTable, resolutionTables, aggregations);
  }

  private List<Aggregation> getAggregations(Map<String, String> properties) {
    // Example of configuring one aggregation with two tags: user and action and user being required:
    //   dataset.cube.aggregation.1.tags=user,action
    //   dataset.cube.aggregation.1.requiredTags=user

    Map<String, List<String>> aggTags = Maps.newHashMap();
    Map<String, Set<String>> aggRequiredTags = Maps.newHashMap();
    for (Map.Entry<String, String> prop : properties.entrySet()) {
      if (prop.getKey().startsWith(PROPERTY_AGGREGATION_PREFIX)) {
        String aggregationProp = prop.getKey().substring(PROPERTY_AGGREGATION_PREFIX.length());
        String[] nameAndProp = aggregationProp.split("\\.", 2);
        if (nameAndProp.length != 2) {
          throw new IllegalArgumentException("Invalid property: " + prop.getKey());
        }
        String[] tags = prop.getValue().split(",");
        if (PROPERTY_TAGS.equals(nameAndProp[1])) {
          aggTags.put(nameAndProp[0], Arrays.asList(tags));
        } else if (PROPERTY_REQUIRED_TAGS.equals(nameAndProp[1])) {
          aggRequiredTags.put(nameAndProp[0], new HashSet<String>(Arrays.asList(tags)));
        } else {
          throw new IllegalArgumentException("Invalid property: " + prop.getKey());
        }
      }
    }

    List<Aggregation> aggregations = Lists.newArrayList();
    for (Map.Entry<String, List<String>> aggTagsEntry : aggTags.entrySet()) {
      Set<String> requiredTags = aggRequiredTags.get(aggTagsEntry.getKey());
      requiredTags = requiredTags == null ? Collections.<String>emptySet() : requiredTags;
      aggregations.add(new DefaultAggregation(aggTagsEntry.getValue(), requiredTags));
    }
    return aggregations;
  }

  private int[] getResolutions(Map<String, String> propsMap) {
    // Example of configuring 1 second and 60 seconds resolutions:
    //   dataset.cube.resolutions=1,60

    String resProp = propsMap.get(PROPERTY_RESOLUTIONS);
    int[] resolutions;
    if (resProp == null) {
      resolutions = DEFAULT_RESOLUTIONS;
    } else {
      String[] seconds = resProp.split(",");
      resolutions = new int[seconds.length];
      for (int i = 0; i < seconds.length; i++) {
        resolutions[i] = Integer.valueOf(seconds[i]);
      }
    }
    return resolutions;
  }
}
