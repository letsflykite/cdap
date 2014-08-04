/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data.dataset;

import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.data.DataSetInstantiationException;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.DataFabric;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.ReactorDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;

import java.io.Closeable;
import java.util.Map;

/**
 * The data set instantiator creates instances of data sets at runtime. It
 * must be called from the execution context to get operational instances
 * of data sets. Given a list of data set specs and a data fabric runtime it
 * can construct an instance of a data set and inject the data fabric runtime
 * into its base tables (and other built-in data sets).
 *
 * The instantiation and injection uses Java reflection a lot. This may look
 * unclean, but it helps us keep the DataSet API clean and simple (no need
 * to pass in data fabric runtime, no exposure of the developer to the raw
 * data fabric, he only interacts with data sets).
 */
public class DataSetInstantiator extends DataSetInstantiationBase implements DataSetContext {

  private final DataFabric fabric;
  private final DatasetFramework datasetFramework;

  /**
   * Constructor from data fabric.
   * @param fabric the data fabric
   * @param classLoader the class loader to use for loading data set classes.
   *                    If null, then the default class loader is used
   */
  public DataSetInstantiator(DataFabric fabric, DatasetFramework datasetFramework,
                             CConfiguration configuration, ClassLoader classLoader) {
    super(configuration, classLoader);
    this.fabric = fabric;
    this.datasetFramework =
      new NamespacedDatasetFramework(datasetFramework,
                                     new ReactorDatasetNamespace(configuration, Namespace.USER));
  }

  @Override
  public <T extends Closeable> T getDataSet(String dataSetName)
    throws DataSetInstantiationException {
    return getDataSet(dataSetName, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  public <T extends Closeable> T getDataSet(String name, Map<String, String> arguments)
    throws DataSetInstantiationException {
    return (T) super.getDataSet(name, arguments, this.fabric, this.datasetFramework);
  }
}
