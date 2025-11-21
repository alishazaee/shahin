package org.shahin.enrichment;

import org.shahin.protobuf.NetRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static org.shahin.utils.EnrichmentError.*;

public class IdentityEnricher {
    private static final Logger logger = LoggerFactory.getLogger(IdentityEnricher.class);

    private void validateSourceProto(NetRecordProto.NetRecord.Builder builder,EnrichmentResult result){
        if (Objects.equals(builder.getIpv4DstAddr(), "")){
            result.addError(DESTINATION_ADDRESS_NULL_PROBLEM);
        } else if(!"^(((?!25?[6-9])[12]\\d|[1-9])?\\d\\.?\\b){4}$".matches(builder.getIpv4DstAddr())){
            result.addError(DESTINATION_ADDRESS_REGEX_PROBLEM);
        }
        if (Objects.equals(builder.getIpv4SrcAddr(), "")){
            result.addError(SOURCE_ADDRESS_NULL_PROBLEM);
        } else if(!"^(((?!25?[6-9])[12]\\d|[1-9])?\\d\\.?\\b){4}$".matches(builder.getIpv4SrcAddr())){
            result.addError(SOURCE_ADDRESS_REGEX_PROBLEM);
        }
        if(Objects.equals(builder.getL4SrcPort(), 0)){
            result.addError(SOURCE_PORT_NULL_PROBLEM);
        } else if(builder.getL4SrcPort() < 1 || builder.getL4SrcPort() > 65535){
            result.addError(SOURCE_PORT_REGEX_PROBLEM);
        }
        if(Objects.equals(builder.getL4DstPort(), 0)){
            result.addError(DESTINATION_PORT_NULL_PROBLEM);
        } else if(builder.getL4DstPort() < 1 || builder.getL4DstPort() > 65535){
            result.addError(DESTINATION_PORT_REGEX_PROBLEM);
        }
        if(Objects.equals(builder.getSenderLatitude(), "")) {
            result.addError(LATITUDE_NULL_PROBLEM);
        }
        if(Objects.equals(builder.getSenderLongitude(), "")){
            result.addError(LONGITUDE_NULL_PROBLEM);
        }
    }


    private Optional<Identity> findIdentity(NetRecordProto.NetRecord.Builder builder){
        return Optional.empty();
    }

    public EnrichmentResult enrich(NetRecordProto.NetRecord.Builder builder){
        EnrichmentResult result = new EnrichmentResult();
        validateSourceProto(builder,result);

        if(!result.hasErrors()){
            Optional<Identity> identity = findIdentity(builder);
            if(identity.isPresent()){
                if(identity.get().getFirstName() != null){
                    builder.setSenderFirstName(identity.get().getFirstName());
                }
                else {
                    result.addError(FIRSTNAME_NOT_FOUND);
                }
                if(identity.get().getLastName() != null){
                    builder.setSenderLastName(identity.get().getLastName());
                }
                else {
                    result.addError(LASTNAME_NOT_FOUND);
                }
                if(identity.get().getNationalID() != null){
                    builder.setSenderNationalId(identity.get().getNationalID());
                }
                else {
                    result.addError(NATIONAL_ID_NOT_FOUND);
                }

            } else {
                result.addError(IDENTITY_NOT_FOUND);
            }
        }
        return result;

    }


}
