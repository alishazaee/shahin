package org.shahin.utils;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import static io.micrometer.core.instrument.Metrics.counter;

public class Profiler {

    private final static String ENRICHMENT_ERROR_TAG_NAME = "enrichment_error_type";
    private final static String ENRICHER_METRIC_PREFIX = "enricher_";

    private final String metricName;
    private Tags tags = Tags.empty();

    public Profiler(TransformStatus transformStatus) {
        this.metricName = ENRICHER_METRIC_PREFIX + transformStatus.name();
    }

    public Profiler withTags(Tags tags) {
        this.tags = tags;
        return this;
    }

    public Profiler withEnrichmentError(EnrichmentError error) {
        tags = tags.and(Tag.of(ENRICHMENT_ERROR_TAG_NAME, error.toString()));
        return this;
    }

    public void increment() {
        counter(metricName, tags).increment();
    }
}
