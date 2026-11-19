package com.banking.transactionservice.entity;

import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

/*
   * Transaction life cycle flow
   * pending -> processing-> completed( Clean Transaction) [Money Credit to receiver]
   * pending -> processing -> pending verification (Suspicious detected)
   *                                                                     if verified  -> COMPLETED (verified)
   *                                                                     else         -> FLAGGED (Saga Refund)
   *                                                                     -> FAILED
   *                                                                       -> FLAGGED
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
