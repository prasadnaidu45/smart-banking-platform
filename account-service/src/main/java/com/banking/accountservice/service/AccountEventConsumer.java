package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
    private final AccountService accountService;
    /*
        Consume transaction completed event from kafka
        no fraud detected and we can credit the amount to the receiver account
 */

    @KafkaListener(topics = "transaction.completed")
    public void oonsumeTransactionCompleted(@Payload Map<String,Object> payload){
        try{
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting Amount to receiver account {} ",receiverAccount);
            accountService.creditBalance(receiverAccount,amount);
        } catch (Exception e) {
            log.error("Error crediting amount {} ",e.getMessage());
        }
    }

    /*
      Consumes Fraud Detected event from kafka
      Blocks the flagged Accounts
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String,Object>payload){
        try{
            String accountNumber = (String)payload.get("accountNumber");
            log.info("Fraud Detected - blocking Account {}",accountNumber);
            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error Blocking account {} ",e.getMessage());
        }
    }
}
