package com.ibm.garage.cpat.cp4i.SchemaValidation;

import com.ibm.garage.cpat.cp4i.FinancialMessage.FinancialMessage;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Broadcast;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.Incoming;



@ApplicationScoped
public class SchemaValidation {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidation.class);

    // @Incoming annotation denotes the incoming channel that we'll be reading from.
    // The @Outgoing denotes the outgoing channel that we'll be sending to.
    @Incoming("pre-schema-check")
    @Outgoing("post-schema-check")
    @Broadcast
    public Flowable<FinancialMessage> processCompliance(FinancialMessage financialMessage) {

        FinancialMessage receivedMessage = financialMessage;

        LOGGER.info("Message received from topic = {}", receivedMessage);

        if (receivedMessage.schema_validation && !receivedMessage.technical_validation &&
            !receivedMessage.business_validation) {
            /*
            Check whether schema_valiation is true as well as if technical_validation (previous) 
            and trade_enrichment (next) are false. If so it's ready to be processed.
            We flip the boolean value to indicate that this service has processed it and ready for the next step. 
            */
            receivedMessage.schema_validation = false;
            receivedMessage.trade_enrichment = true;
        
            return Flowable.just(receivedMessage);
        }

        else {
            return Flowable.empty();
        }

        // return (receivedMessage.compliance_services) ? Flowable.just(complianceCheckComplete(receivedMessage)) : Flowable.empty();
    }
}