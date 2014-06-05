package com.continuuity.common.conf;

import java.util.concurrent.TimeUnit;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {
  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String APP_FABRIC_HTTP = "appfabric";
    public static final String TRANSACTION = "transaction";
    public static final String METRICS = "metrics";
    public static final String GATEWAY = "gateway";
    public static final String METRICS_PROCESSOR = "metrics.processor";
    public static final String STREAM_HANDLER = "stream.handler";
    public static final String DATASET_MANAGER = "dataset.manager";
    public static final String DATASET_EXECUTOR = "dataset.executor";
    public static final String EXTERNAL_AUTHENTICATION = "external.authentication";
    public static final String HIVE = "hive.server";
  }

  /**
   * Zookeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String CFG_SESSION_TIMEOUT_MILLIS = "zookeeper.session.timeout.millis";
    public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 40000;
  }

  /**
   * HBase configurations.
   */
  public static final class HBase {
    public static final String AUTH_KEY_UPDATE_INTERVAL = "hbase.auth.key.update.interval";
  }

  /**
   * Thrift configuration.
   */
  public static final class Thrift {
    public static final String MAX_READ_BUFFER = "thrift.max.read.buffer";
    public static final int DEFAULT_MAX_READ_BUFFER = 16 * 1024 * 1024;
  }

  /**
   * Dangerous Options.
   */
  public static final class Dangerous {
    public static final String UNRECOVERABLE_RESET = "enable.unrecoverable.reset";
    public static final boolean DEFAULT_UNRECOVERABLE_RESET = false;
  }

  /**
   * App Fabric Configuration.
   */
  public static final class AppFabric {
    /**
     * Default constants for common.
     */

    //TODO: THis temp
    public static final String DEFAULT_SERVER_ADDRESS = "localhost";

    /**
     * App Fabric Server.
     */
    public static final String SERVER_ADDRESS = "app.bind.address";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";

    /**
     * Query parameter to indicate start time.
     */
    public static final String QUERY_PARAM_START_TIME = "before";

    /**
     * Query parameter to indicate end time.
     */
    public static final String QUERY_PARAM_END_TIME = "after";

    /**
     * Query parameter to indicate limits on results.
     */
    public static final String QUERY_PARAM_LIMIT = "limit";

    /**
     * Default history results limit.
     */
    public static final int DEFAULT_HISTORY_RESULTS_LIMIT = 100;
  }

  /**
   * Scheduler options.
   */
  public class Scheduler {
    public static final String CFG_SCHEDULER_MAX_THREAD_POOL_SIZE = "scheduler.max.thread.pool.size";
    public static final int DEFAULT_THREAD_POOL_SIZE = 30;
  }

  /**
   * Transactions.
   */
  public static final class Transaction {
    /**
     * TransactionManager configuration.
     */
    public static final class Manager {
      // TransactionManager configuration
      public static final String CFG_DO_PERSIST = "tx.persist";
      /** Directory in HDFS used for transaction snapshot and log storage. */
      public static final String CFG_TX_SNAPSHOT_DIR = "data.tx.snapshot.dir";
      /** Directory on the local filesystem used for transaction snapshot and log storage. */
      public static final String CFG_TX_SNAPSHOT_LOCAL_DIR = "data.tx.snapshot.local.dir";
      /** How often to clean up timed out transactions, in seconds, or 0 for no cleanup. */
      public static final String CFG_TX_CLEANUP_INTERVAL = "data.tx.cleanup.interval";
      /** Default value for how often to check in-progress transactions for expiration, in seconds. */
      public static final int DEFAULT_TX_CLEANUP_INTERVAL = 10;
      /**
       * The timeout for a transaction, in seconds. If the transaction is not finished in that time,
       * it is marked invalid.
       */
      public static final String CFG_TX_TIMEOUT = "data.tx.timeout";
      /** Default value for transaction timeout, in seconds. */
      public static final int DEFAULT_TX_TIMEOUT = 30;
      /** The frequency (in seconds) to perform periodic snapshots, or 0 for no periodic snapshots. */
      public static final String CFG_TX_SNAPSHOT_INTERVAL = "data.tx.snapshot.interval";
      /** Default value for frequency of periodic snapshots of transaction state. */
      public static final long DEFAULT_TX_SNAPSHOT_INTERVAL = 300;
      /** Number of most recent transaction snapshots to retain. */
      public static final String CFG_TX_SNAPSHOT_RETAIN = "data.tx.snapshot.retain";
      /** Default value for number of most recent snapshots to retain. */
      public static final int DEFAULT_TX_SNAPSHOT_RETAIN = 10;
    }

    /**
     * Twill Runnable configuration.
     */
    public static final class Container {
      public static final String ADDRESS = "data.tx.bind.address";
      public static final String NUM_INSTANCES = "data.tx.num.instances";
      public static final String NUM_CORES = "data.tx.num.cores";
      public static final String MEMORY_MB = "data.tx.memory.mb";
    }

    /**
     * TransactionService configuration.
     */
    public static final class Service {
  
      /** for the port of the tx server. */
      public static final String CFG_DATA_TX_BIND_PORT
        = "data.tx.bind.port";
  
      /** for the address (hostname) of the tx server. */
      public static final String CFG_DATA_TX_BIND_ADDRESS
        = "data.tx.bind.address";

      /** the number of IO threads in the tx service. */
      public static final String CFG_DATA_TX_SERVER_IO_THREADS
        = "data.tx.server.io.threads";
  
      /** the number of handler threads in the tx service. */
      public static final String CFG_DATA_TX_SERVER_THREADS
        = "data.tx.server.threads";
  
      /** default tx service port. */
      public static final int DEFAULT_DATA_TX_BIND_PORT
        = 15165;
  
      /** default tx service address. */
      public static final String DEFAULT_DATA_TX_BIND_ADDRESS
        = "0.0.0.0";
  
      /** default number of handler IO threads in tx service. */
      public static final int DEFAULT_DATA_TX_SERVER_IO_THREADS
        = 2;
  
      /** default number of handler threads in tx service. */
      public static final int DEFAULT_DATA_TX_SERVER_THREADS
        = 20;
  
      // Configuration key names and defaults used by tx client.
  
      /** to specify the tx client socket timeout in ms. */
      public static final String CFG_DATA_TX_CLIENT_TIMEOUT
        = "data.tx.client.timeout";

      /** to specify the tx client provider strategy. */
      public static final String CFG_DATA_TX_CLIENT_PROVIDER
        = "data.tx.client.provider";
  
      /** to specify the number of threads for client provider "pool". */
      public static final String CFG_DATA_TX_CLIENT_COUNT
        = "data.tx.client.count";
  
      /** to specify the retry strategy for a failed thrift call. */
      public static final String CFG_DATA_TX_CLIENT_RETRY_STRATEGY
        = "data.tx.client.retry.strategy";
  
      /** to specify the number of times to retry a failed thrift call. */
      public static final String CFG_DATA_TX_CLIENT_ATTEMPTS
        = "data.tx.client.retry.attempts";
  
      /** to specify the initial sleep time for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_INIITIAL
        = "data.tx.client.retry.backoff.initial";
  
      /** to specify the backoff factor for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_FACTOR
        = "data.tx.client.retry.backoff.factor";
  
      /** to specify the sleep time limit for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_LIMIT
        = "data.tx.client.retry.backoff.limit";
  
      /** the default tx client socket timeout in milli seconds. */
      public static final int DEFAULT_DATA_TX_CLIENT_TIMEOUT
        = 30 * 1000;

      /** default number of pooled tx clients. */
      public static final int DEFAULT_DATA_TX_CLIENT_COUNT
        = 5;
  
      /** default tx client provider strategy. */
      public static final String DEFAULT_DATA_TX_CLIENT_PROVIDER
        = "pool";
  
      /** retry strategy for thrift clients, e.g. backoff, or n-times. */
      public static final String DEFAULT_DATA_TX_CLIENT_RETRY_STRATEGY
        = "backoff";
  
      /** default number of attempts for strategy n-times. */
      public static final int DEFAULT_DATA_TX_CLIENT_ATTEMPTS
        = 2;
  
      /** default initial sleep is 100ms. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_INIITIAL
        = 100;
  
      /** default backoff factor is 4. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_FACTOR
        = 4;
  
      /** default sleep limit is 30 sec. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_LIMIT
        = 30 * 1000;
    }

    /**
     * Configuration for the TransactionDataJanitor coprocessor.
     */
    public static final class DataJanitor {
      /**
       * Whether or not the TransactionDataJanitor coprocessor should be enabled on tables.
       * Disable for testing.
       */
      public static final String CFG_TX_JANITOR_ENABLE = "data.tx.janitor.enable";
      public static final boolean DEFAULT_TX_JANITOR_ENABLE = true;
    }
  }

  /**
   * Datasets.
   */
  public static final class Dataset {
    /**
     * DatasetManager service configuration.
     */
    public static final class Manager {
      /** for the port of the dataset server. */
      public static final String PORT = "dataset.service.bind.port";

      /** for the address (hostname) of the dataset server. */
      public static final String ADDRESS = "dataset.service.bind.address";

      public static final String BACKLOG_CONNECTIONS = "dataset.service.connection.backlog";
      public static final String EXEC_THREADS = "dataset.service.exec.threads";
      public static final String BOSS_THREADS = "dataset.service.boss.threads";
      public static final String WORKER_THREADS = "dataset.service.worker.threads";
      public static final String OUTPUT_DIR = "dataset.service.output.dir";

      // Defaults
      public static final int DEFAULT_BACKLOG = 20000;
      public static final int DEFAULT_EXEC_THREADS = 10;
      public static final int DEFAULT_BOSS_THREADS = 1;
      public static final int DEFAULT_WORKER_THREADS = 4;
    }

    /**
     * Twill Runnable configuration.
     */
    public static final class Container {
      public static final String NUM_INSTANCES = "dataset.service.num.instances";
      public static final String NUM_CORES = "dataset.service.num.cores";
      public static final String MEMORY_MB = "dataset.service.memory.mb";
    }

    /**
     * DatasetUserService configuration.
     */
    public static final class Executor {
      /** for the port of the dataset user service server. */
      public static final String PORT = "dataset.executor.bind.port";

      /** for the address (hostname) of the dataset server. */
      public static final String ADDRESS = "dataset.executor.bind.address";

      public static final String BACKLOG_CONNECTIONS = "dataset.executor.connection.backlog";
      public static final String EXEC_THREADS = "dataset.executor.exec.threads";
      public static final String BOSS_THREADS = "dataset.executor.boss.threads";
      public static final String WORKER_THREADS = "dataset.executor.worker.threads";
      public static final String OUTPUT_DIR = "dataset.executor.output.dir";

      /** Twill Runnable configuration **/
      public static final String CONTAINER_VIRTUAL_CORES = "dataset.executor.container.num.cores";
      public static final String CONTAINER_MEMORY_MB = "dataset.executor.container.memory.mb";
      public static final String CONTAINER_INSTANCES = "dataset.executor.container.instances";
    }
  }

  /**
   * Stream configurations.
   */
  public static final class Stream {
    /* Begin CConfiguration keys */
    public static final String BASE_DIR = "stream.base.dir";
    public static final String TTL = "stream.event.ttl";
    public static final String PARTITION_DURATION = "stream.partition.duration";
    public static final String INDEX_INTERVAL = "stream.index.interval";
    public static final String FILE_PREFIX = "stream.file.prefix";
    public static final String CONSUMER_TABLE_PRESPLITS = "stream.consumer.table.presplits";

    // Stream http service configurations.
    public static final String ADDRESS = "stream.bind.address";
    public static final String WORKER_THREADS = "stream.worker.threads";

    // YARN container configurations.
    public static final String CONTAINER_VIRTUAL_CORES = "stream.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "stream.container.memory.mb";
    public static final String CONTAINER_INSTANCES = "stream.container.instances";

    // Tell the instance id of the YARN container. Set by the StreamHandlerRunnable only, not in default.xml
    public static final String CONTAINER_INSTANCE_ID = "stream.container.instance.id";
    /* End CConfiguration keys */

    /* Begin constants used by stream */

    /** How often to check for new file when reading from stream in milliseconds. **/
    public static final long NEW_FILE_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    public static final int HBASE_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;


    /**
     * Contains HTTP headers used by Stream handler.
     */
    public static final class Headers {
      public static final String CONSUMER_ID = "X-Continuuity-ConsumerId";
    }

    // Time for a stream consumer to timeout in StreamHandler for REST API dequeue.
    public static final long CONSUMER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    // The consumer state table namespace for consumers created from stream handler for REST API dequeue.
    public static final String HANDLER_CONSUMER_NS = "http.stream.consumer";

    /* End constants used by stream */
  }

  /**
   * Gateway Configurations.
   */
  public static final class Gateway {
    public static final String ADDRESS = "gateway.bind.address";
    public static final String PORT = "gateway.bind.port";
    public static final String BACKLOG_CONNECTIONS = "gateway.connection.backlog";
    public static final String EXEC_THREADS = "gateway.exec.threads";
    public static final String BOSS_THREADS = "gateway.boss.threads";
    public static final String WORKER_THREADS = "gateway.worker.threads";
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "gateway.authenticate";
    public static final String CLUSTER_NAME = "gateway.cluster.name";
    public static final String NUM_CORES = "gateway.num.cores";
    public static final String NUM_INSTANCES = "gateway.num.instances";
    public static final String MEMORY_MB = "gateway.memory.mb";
    public static final String STREAM_FLUME_THREADS = "stream.flume.threads";
    public static final String STREAM_FLUME_PORT = "stream.flume.port";
    /**
     * Defaults.
     */
    public static final int DEFAULT_PORT = 10000;
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_EXEC_THREADS = 20;
    public static final int DEFAULT_BOSS_THREADS = 1;
    public static final int DEFAULT_WORKER_THREADS = 10;
    public static final boolean CONFIG_AUTHENTICATION_REQUIRED_DEFAULT = false;
    public static final String CLUSTER_NAME_DEFAULT = "localhost";
    public static final int DEFAULT_NUM_CORES = 2;
    public static final int DEFAULT_NUM_INSTANCES = 1;
    public static final int DEFAULT_MEMORY_MB = 2048;
    public static final int DEFAULT_STREAM_FLUME_THREADS = 10;
    public static final int DEFAULT_STREAM_FLUME_PORT = 10004;


    /**
     * Others.
     */
    public static final String GATEWAY_VERSION = "/v2";
    public static final String CONTINUUITY_PREFIX = "X-Continuuity-";
    public static final String STREAM_HANDLER_NAME = "stream.rest";
    public static final String METRICS_CONTEXT = "gateway." + Gateway.STREAM_HANDLER_NAME;
    public static final String HEADER_DESTINATION_STREAM = "X-Continuuity-Destination";
    public static final String HEADER_FROM_COLLECTOR = "X-Continuuity-FromCollector";
    public static final String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";
    public static final String CFG_PASSPORT_SERVER_URI = "passport.server.uri";
  }

  /**
   * Router Configuration.
   */
  public static final class Router {
    public static final String ADDRESS = "router.bind.address";
    public static final String FORWARD = "router.forward.rule";
    public static final String BACKLOG_CONNECTIONS = "router.connection.backlog";
    public static final String SERVER_BOSS_THREADS = "router.server.boss.threads";
    public static final String SERVER_WORKER_THREADS = "router.server.worker.threads";
    public static final String CLIENT_BOSS_THREADS = "router.client.boss.threads";
    public static final String CLIENT_WORKER_THREADS = "router.client.worker.threads";

    /**
     * Defaults.
     */
    public static final String DEFAULT_FORWARD = "10000:" + Service.GATEWAY;
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_SERVER_BOSS_THREADS = 1;
    public static final int DEFAULT_SERVER_WORKER_THREADS = 10;
    public static final int DEFAULT_CLIENT_BOSS_THREADS = 1;
    public static final int DEFAULT_CLIENT_WORKER_THREADS = 10;
  }

  /**
   * Webapp Configuration.
   */
  public static final class Webapp {
    public static final String WEBAPP_DIR = "webapp";
  }

  /**
   * Metrics constants.
   */
  public static final class Metrics {
    public static final String DATASET_CONTEXT = "-.dataset";
    public static final String ADDRESS = "metrics.bind.address";
    public static final String CLUSTER_NAME = "metrics.cluster.name";
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "metrics.authenticate";
    public static final String BACKLOG_CONNECTIONS = "metrics.connection.backlog";
    public static final String EXEC_THREADS = "metrics.exec.threads";
    public static final String BOSS_THREADS = "metrics.boss.threads";
    public static final String WORKER_THREADS = "metrics.worker.threads";
    public static final String NUM_INSTANCES = "metrics.num.instances";
    public static final String NUM_CORES = "metrics.num.cores";
    public static final String MEMORY_MB = "metrics.memory.mb";
  }

  /**
   * Configurations for metrics processor.
   */
  public static final class MetricsProcessor {
    public static final String NUM_INSTANCES = "metrics.processor.num.instances";
    public static final String NUM_CORES = "metrics.processor.num.cores";
    public static final String MEMORY_MB = "metrics.processor.memory.mb";
  }

  /**
   * Configurations for log saver.
   */
  public static final class LogSaver {
    public static final String NUM_INSTANCES = "log.saver.num.instances";
    public static final String MEMORY_MB = "log.saver.run.memory.megs";
  }

  /**
   *
   */
  public static final class Logging {
    public static final String SYSTEM_NAME = "reactor";
    public static final String COMPONENT_NAME = "services";
  }

  /**
   * Security configuration.
   */
  public static final class Security {
    /** Algorithm used to generate the digest for access tokens. */
    public static final String TOKEN_DIGEST_ALGO = "security.token.digest.algorithm";
    /** Key length for secret key used by token digest algorithm. */
    public static final String TOKEN_DIGEST_KEY_LENGTH = "security.token.digest.keylength";
    /** Time duration in milliseconds after which an active secret key should be retired. */
    public static final String TOKEN_DIGEST_KEY_EXPIRATION = "security.token.digest.key.expiration.ms";
    /** Parent znode used for secret key distribution in ZooKeeper. */
    public static final String DIST_KEY_PARENT_ZNODE = "security.token.distributed.parent.znode";
    /** Address the Authentication Server should bind to*/
    public static final String AUTH_SERVER_ADDRESS = "security.auth.server.address";
    /** Configuration for External Authentication Server. */
    public static final String AUTH_SERVER_PORT = "security.auth.server.port";
    /** Maximum number of handler threads for the Authentication Server embedded Jetty instance. */
    public static final String MAX_THREADS = "security.server.maxthreads";
    /** Access token expiration time in milliseconds. */
    public static final String TOKEN_EXPIRATION = "security.server.token.expiration.ms";
    public static final String[] BASIC_USER_ROLES = new String[] {"user", "admin", "moderator"};

    public static final String CFG_FILE_BASED_KEYFILE_PATH = "security.data.keyfile.path";
    /** Configuration for enabling the security. */
    public static final String CFG_SECURITY_ENABLED = "security.enabled";
    /** Configuration for security realm. */
    public static final String CFG_REALM = "security.realm";
    /** Authentication Handler class name */
    public static final String AUTH_HANDLER_CLASS = "security.authentication.handlerClassName";
    /** Prefix for all configurable properties of an Authentication handler. */
    public static final String AUTH_HANDLER_CONFIG_BASE = "security.authentication.handler.";
    /** Authentication Login Module class name */
    public static final String LOGIN_MODULE_CLASS_NAME = "security.authentication.loginmodule.className";
    /** Configuration for enabling SSL */
    public static final String SSL_ENABLED = "security.server.ssl.enabled";
    /** SSL secured port for ExternalAuthentication */
    public static final String AUTH_SERVER_SSL_PORT = "security.server.ssl.port";
    /** SSL keystore path */
    public static final String SSL_KEYSTORE_PATH = "security.server.ssl.keystore.path";
    /** SSL keystore password */
    public static final String SSL_KEYSTORE_PASSWORD = "security.server.ssl.keystore.password";
    /** Realm file for Basic Authentication */
    public static final String BASIC_REALM_FILE = "security.authentication.basic.realmfile";
    /** Header base for requests send downstream from Router*/
    public static final String VERIFIED_HEADER_BASE = "Reactor-verified ";
  }

  /**
   * Hive configuration.
   */
  public static final class Hive {
    public static final String SERVER_ADDRESS = "hive.server.bind.address";
    public static final String SERVER_PORT = "hive.server.port";
    public static final String METASTORE_PORT = "hive.local.metastore.port";
    public static final String METASTORE_WAREHOUSE_DIR = "reactor.hive.metastore.warehouse.dir";
    public static final String DATABASE_DIR = "reactor.hive.database.dir";
  }

  /**
   * Explore module configuration.
   */
  public static final class Explore {
    public static final String CCONF_CODEC_KEY = "reactor.cconfiguration";
    public static final String HCONF_CODEC_KEY = "reactor.hconfiguration";
    public static final String TX_QUERY_CODEC_KEY = "reactor.hive.query.tx.id";

    public static final String DATASET_NAME = "reactor.dataset.name";
    public static final String DATASET_STORAGE_HANDLER_CLASS = "com.continuuity.hive.datasets.DatasetStorageHandler";
  }

  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_TWILL_ZK_NAMESPACE = "weave.zookeeper.namespace";
  public static final String CFG_TWILL_RESERVED_MEMORY_MB = "weave.java.reserved.memory.mb";
  public static final String CFG_TWILL_NO_CONTAINER_TIMEOUT = "weave.no.container.timeout";

  /**
   * Data Fabric.
   */
  public static enum InMemoryPersistenceType {
    MEMORY,
    LEVELDB,
    HSQLDB
  }
  /** defines which persistence engine to use when running all in one JVM. **/
  public static final String CFG_DATA_INMEMORY_PERSISTENCE = "data.local.inmemory.persistence.type";
  public static final String CFG_DATA_LEVELDB_DIR = "data.local.storage";
  public static final String CFG_DATA_LEVELDB_BLOCKSIZE = "data.local.storage.blocksize";
  public static final String CFG_DATA_LEVELDB_CACHESIZE = "data.local.storage.cachesize";
  public static final String CFG_DATA_LEVELDB_FSYNC = "data.local.storage.fsync";


  /**
   * Defaults for Data Fabric.
   */
  public static final String DEFAULT_DATA_INMEMORY_PERSISTENCE = InMemoryPersistenceType.MEMORY.name();
  public static final String DEFAULT_DATA_LEVELDB_DIR = "data";
  public static final int DEFAULT_DATA_LEVELDB_BLOCKSIZE = 1024;
  public static final long DEFAULT_DATA_LEVELDB_CACHESIZE = 1024 * 1024 * 100;
  public static final boolean DEFAULT_DATA_LEVELDB_FSYNC = true;

  /**
   * Configuration for Metadata service.
   */
  public static final String CFG_RUN_HISTORY_KEEP_DAYS = "metadata.program.run.history.keepdays";
  public static final int DEFAULT_RUN_HISTORY_KEEP_DAYS = 30;

  /**
   * Config for Log Collection.
   */
  public static final String CFG_LOG_COLLECTION_ROOT = "log.collection.root";
  public static final String DEFAULT_LOG_COLLECTION_ROOT = "data/logs";
  public static final String CFG_LOG_COLLECTION_PORT = "log.collection.bind.port";
  public static final int DEFAULT_LOG_COLLECTION_PORT = 12157;
  public static final String CFG_LOG_COLLECTION_THREADS = "log.collection.threads";
  public static final int DEFAULT_LOG_COLLECTION_THREADS = 10;
  public static final String CFG_LOG_COLLECTION_SERVER_ADDRESS = "log.collection.bind.address";
  public static final String DEFAULT_LOG_COLLECTION_SERVER_ADDRESS = "localhost";

  /**
   * Constants related to Passport.
   */
  public static final String CFG_APPFABRIC_ENVIRONMENT = "appfabric.environment";
  public static final String DEFAULT_APPFABRIC_ENVIRONMENT = "devsuite";


  /**
   * Corresponds to account id used when running in local mode.
   * NOTE: value should be in sync with the one used by UI.
   */
  public static final String DEVELOPER_ACCOUNT_ID = "developer";
}
