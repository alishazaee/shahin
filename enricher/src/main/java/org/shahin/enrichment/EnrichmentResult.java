package org.shahin.enrichment;

import org.shahin.utils.EnrichmentError;

import java.util.ArrayList;
import java.util.List;

public class EnrichmentResult {
    private List<EnrichmentError> errors = new ArrayList<>();

    public List<EnrichmentError> getError() {
        return errors;
    }

    public EnrichmentResult addError(EnrichmentError error) {
        this.errors.add(error);
        return this;
    }
    public Boolean hasErrors() {
        return !errors.isEmpty();
    }
}
