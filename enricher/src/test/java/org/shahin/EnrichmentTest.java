package org.shahin;

import org.junit.jupiter.api.Test;
import org.shahin.enrichment.EnrichmentResult;
import org.shahin.enrichment.IdentityEnricher;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.utils.EnrichmentError;

import static org.junit.jupiter.api.Assertions.*;

public class EnrichmentTest {
    private final IdentityEnricher identityEnricher;

    public EnrichmentTest(){
        identityEnricher = new IdentityEnricher();
    }

    @Test
    public void shouldReturnErrorForEmptyFields(){
        NetRecordProto.NetRecord.Builder log =
                NetRecordProto.NetRecord.newBuilder()
                        .setIpv4SrcAddr("")
                        .setIpv4DstAddr("")
                        .setL4SrcPort(0)
                        .setL4DstPort(0)
                        .setSenderLatitude("324234")
                        .setSenderLongitude("324234");
        EnrichmentResult enrichmentResult = identityEnricher.enrich(log);
        assertTrue(enrichmentResult.hasErrors());
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.SOURCE_ADDRESS_NULL_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.DESTINATION_ADDRESS_NULL_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.SOURCE_PORT_NULL_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.DESTINATION_PORT_NULL_PROBLEM));
        assertEquals(4, enrichmentResult.getError().size());
    }

    @Test
    public void shouldReturnErrorForInvalidFields(){
        NetRecordProto.NetRecord.Builder log =
                NetRecordProto.NetRecord.newBuilder()
                        .setIpv4SrcAddr("552.123.53.256")
                        .setIpv4DstAddr("notValidIP")
                        .setL4SrcPort(-2)
                        .setL4DstPort(996756)
                        .setSenderLatitude("324234")
                        .setSenderLongitude("324234");
        EnrichmentResult enrichmentResult = identityEnricher.enrich(log);
        assertTrue(enrichmentResult.hasErrors());
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.SOURCE_ADDRESS_REGEX_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.DESTINATION_ADDRESS_REGEX_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.SOURCE_PORT_REGEX_PROBLEM));
        assertTrue(enrichmentResult.getError().contains(EnrichmentError.DESTINATION_PORT_REGEX_PROBLEM));
        assertEquals(4, enrichmentResult.getError().size());

    }
}
