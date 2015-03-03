/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.config.ConfigStore;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.DefaultStoreFactory;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.runtime.TransactionModules;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Command line tool.
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private Store store;

  private final Injector injector;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrade all tables."),
    HELP("Show this help.");

    private final String description;

    private Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  public Main() {
    cConf = CConfiguration.create();
    hConf = HBaseConfiguration.create();

    this.injector = init();
    txService = injector.getInstance(TransactionService.class);
    zkClientService = injector.getInstance(ZKClientService.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Main.this.stop();
        } catch (Throwable e) {
          LOG.error("Failed to upgrade", e);
        }
      }
    });
  }

  private Injector init() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          install(new TransactionModules().getDistributedModules());
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
          bind(StoreFactory.class).to(DefaultStoreFactory.class);
          bind(ConfigStore.class).to(DefaultConfigStore.class);
        }

        @Provides
        @Singleton
        @Named("namespacedDSFramework")
        public DatasetFramework getNamespacedDSFramework(CConfiguration cConf,
                                                         DatasetDefinitionRegistryFactory registryFactory)
          throws IOException, DatasetManagementException {
          return createRegisteredDatasetFramework(cConf, registryFactory);
        }
      });
  }

  /**
   * Do the start up work
   */
  private void startUp() {
    // Start all the services.
    zkClientService.startAndWait();
    txService.startAndWait();

    createDefaultNamespace();
  }

  /**
   * Stop services and
   */
  private void stop() {
    LOG.info("Stopping Upgrade ...");
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
    } catch (Throwable e) {
      LOG.error("Exception while trying to stop upgrade process", e);
      Runtime.getRuntime().halt(1);
    }
  }

  private void doMain(String[] args) throws Exception {
    System.out.println(String.format("%s - version %s.", getClass().getSimpleName(), ProjectInfo.getVersion()));
    System.out.println();

    if (args.length < 1) {
      printHelp();
      return;
    }

    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return;
    }

    try {
      switch (action) {
        case UPGRADE:
          performUpgrade();
          break;
        case HELP:
          printHelp();
          break;
      }
    } catch (Exception e) {
      System.out.println(String.format("Failed to perform action '%s'. Reason: '%s'.", action, e.getMessage()));
      e.printStackTrace(System.out);
      throw e;
    }
  }

  private void printHelp() {
    printHelp(false);
  }

  private void printHelp(boolean beginNewLine) {
    if (beginNewLine) {
      System.out.println();
    }
    System.out.println("Available actions: ");
    System.out.println();

    for (Action action : Action.values()) {
      System.out.println(String.format("  %s - %s", action.name().toLowerCase(), action.getDescription()));
    }
  }

  private Action parseAction(String action) {
    try {
      return Action.valueOf(action.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private void performUpgrade() throws Exception {
    LOG.info("Upgrading System and User Datasets ...");
    DatasetUpgrader dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    dsUpgrade.upgrade();
  }

  public static void main(String[] args) throws Exception {
    Main thisMain = new Main();
    thisMain.startUp();
    try {
      thisMain.doMain(args);
    } catch (Throwable t) {
      LOG.info("Failed to upgrade ...");
    } finally {
      thisMain.stop();
    }
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  private DatasetFramework createRegisteredDatasetFramework(CConfiguration cConf,
                                                            DatasetDefinitionRegistryFactory registryFactory)
    throws DatasetManagementException, IOException {
    DatasetFramework datasetFramework =
      new NamespacedDatasetFramework(new InMemoryDatasetFramework(registryFactory),
                                     new DefaultDatasetNamespace(cConf));
    addModules(datasetFramework);
    // dataset service
    DatasetMetaTableUtil.setupDatasets(datasetFramework);
    // app metadata
    DefaultStore.setupDatasets(datasetFramework);
    // config store
    DefaultConfigStore.setupDatasets(datasetFramework);
    // logs metadata
    LogSaverTableUtil.setupDatasets(datasetFramework);
    // scheduler metadata
    ScheduleStoreTableUtil.setupDatasets(datasetFramework);

    // metrics data
    DefaultMetricDatasetFactory.setupDatasets(cConf, datasetFramework);

    return datasetFramework;
  }

  /**
   * add module to the dataset framework
   *
   * @param datasetFramework the dataset framework to which the modules need to be added
   * @throws DatasetManagementException
   */
  private void addModules(DatasetFramework datasetFramework) throws DatasetManagementException {
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "table"),
                               new HBaseTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "metricsTable"),
                               new HBaseMetricsTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "core"), new CoreDatasetsModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "fileSet"), new FileSetModule());
  }

  /**
   * Creates the {@link Constants#DEFAULT_NAMESPACE} namespace
   */
  private void createDefaultNamespace() {
    getStore().createNamespace(new NamespaceMeta.Builder().setId(Constants.DEFAULT_NAMESPACE)
                                 .setName(Constants.DEFAULT_NAMESPACE)
                                 .setDescription(Constants.DEFAULT_NAMESPACE)
                                 .build());
  }

  /**
   * gets the Store to access the app meta table
   *
   * @return {@link Store}
   */
  private Store getStore() {
    if (store == null) {
      store = injector.getInstance(Key.get(Store.class, Names.named("nonNamespacedStore")));
    }
    return store;
  }
}
