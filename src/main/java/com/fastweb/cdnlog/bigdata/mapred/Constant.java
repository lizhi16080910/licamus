package com.fastweb.cdnlog.bigdata.mapred;

/**
 * Created by lfq on 2016/10/11.
 */
public class Constant {
    public static final String ETL_DESTINATION_PATH = "etl.destination.path";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT = "etl.destination.path.topic.sub.dirformat";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT_LOCALE = "etl.destination.path.topic.sub.dirformat.locale";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
    public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
    public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
    public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

    public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
    public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
    public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
    public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";
    public static final String ETL_RECORD_WRITER_PROVIDER_CLASS = "etl.record.writer.provider.class";


    public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
    public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

    public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";
    public static final String KAFKA_MOVE_TO_EARLIEST_OFFSET = "kafka.move.to.earliest.offset";

    public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
    public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

    public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
    public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
    public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

    public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
    public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
    public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

    public static final String CAMUS_WORK_ALLOCATOR_CLASS = "camus.work.allocator.class";
    public static final String CAMUS_WORK_ALLOCATOR_DEFAULT = "com.linkedin.camus.workallocater.BaseAllocator";

    public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
    public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
    public static final String ETL_COUNTS_PATH = "etl.counts.path";
    public static final String ETL_COUNTS_CLASS = "etl.counts.class";
    public static final String ETL_COUNTS_CLASS_DEFAULT = "com.linkedin.camus.etl.kafka.common.EtlCounts";
    public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
    public static final String ETL_BASEDIR_QUOTA_OVERIDE = "etl.basedir.quota.overide";
    public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
    public static final String ETL_FAIL_ON_ERRORS = "etl.fail.on.errors";
    public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE = "etl.fail.on.offset.outofrange";
    public static final String ETL_FAIL_ON_OFFSET_OUTOFRANGE_DEFAULT = Boolean.TRUE.toString();
    public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND = "etl.max.percent.skipped.schemanotfound";
    public static final String ETL_MAX_PERCENT_SKIPPED_SCHEMANOTFOUND_DEFAULT = "0.1";
    public static final String ETL_MAX_PERCENT_SKIPPED_OTHER = "etl.max.percent.skipped.other";
    public static final String ETL_MAX_PERCENT_SKIPPED_OTHER_DEFAULT = "0.1";
    public static final String ETL_MAX_ERRORS_TO_PRINT_FROM_FILE = "etl.max.errors.to.print.from.file";
    public static final String ETL_MAX_ERRORS_TO_PRINT_FROM_FILE_DEFAULT = "10";
    public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
    public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
    public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
    public static final String BROKER_URI_FILE = "brokers.uri";
    public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
    public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
    public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
    public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
    public static final String KAFKA_CLIENT_NAME = "kafka.client.name";
    public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_HOST_URL = "kafka.host.url";
    public static final String KAFKA_HOST_PORT = "kafka.host.port";
    public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
    public static final String CAMUS_REPORTER_CLASS = "etl.reporter.class";
    public static final String LOG4J_CONFIGURATION = "log4j.configuration";
    public static final String CAMUS_JOB_NAME = "camus.job.name";

    public static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
}
