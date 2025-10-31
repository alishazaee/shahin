package org.shahin;

public class MetricNames {
    public static final String METRIC_NAME_PREFIX = "parquet-writer.";
    public static final String WRITTEN_RECORDS = METRIC_NAME_PREFIX + "written.records";
    public static final String WRITTEN_BYTES = METRIC_NAME_PREFIX + "written.bytes";
    public static final String FLUSHED_PARQUETS = METRIC_NAME_PREFIX + "flushed.parquets";
    public static final String TIMEOUT_PARQUETS = METRIC_NAME_PREFIX + "timeout.parquets";
    public static final String SIZE_EXCEED_PARQUETS = METRIC_NAME_PREFIX + "size_exceed.parquets";
}
