package org.shahin.transformer;

import org.shahin.enrichment.EnrichmentResult;
import org.shahin.enrichment.IdentityEnricher;
import org.shahin.protobuf.NetRecordProto;

public class IdentityTransformer {

    public void transformerResult(){
    }

    public void transform(NetRecordProto.NetRecord.Builder builder){

        IdentityEnricher enricher = new IdentityEnricher();
        EnrichmentResult enrichmentResult = enricher.enrich(builder);

    }
}
