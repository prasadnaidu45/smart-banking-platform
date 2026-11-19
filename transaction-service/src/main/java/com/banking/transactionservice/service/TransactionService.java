package com.banking.transactionservice.service;
import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final RedisTemplate<String,String> redisTemplate;
    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /*
      * SAGA STEP -1 Initiate Transfer
      * Deducts from sender via feign (account service)
      * saves transaction as processing
      * publish event to kafka for fraud check
      * returns;
      *
     */
    public TransactionResponse transfer(TransferRequest request){
        log.info("SAGA Start - Transfer from this {} account to this {} account", request.getSenderAccountNumber(),request.getReceiverAccountNumber());
        // deduct from sender
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        // save transaction as processing
        Transaction transaction = Transaction.builder()
                .senderAccountNumber(request.getSenderAccountNumber())
                .receiverAccountNumber(request.getReceiverAccountNumber())
                .amount(request.getAmount())
                .type(TransactionType.TRANSFER)
                .status(TransactionStatus.PROCESSING)
                .description(request.getDescription())
                .referenceNumber(UUID.randomUUID().toString())
                .build();

        Transaction savedTransaction = transactionRepository.save(transaction);

        log.info("Transaction saved as Processing : {} ",savedTransaction.getId());

        // Saga Step 2 : Publish this event to fraud detection service for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription());

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(),event);

        log.info("Transaction Initiated Event Published :{} ",savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId){
        return mapToResponse(transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException(" Transaction Not Found : "+transactionId)));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                                    .stream()
                                    .map(this::mapToResponse)
                                    .collect(Collectors.toList());
    }

    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(savedTransaction.getId());
        transactionResponse.setSenderAccountNumber(savedTransaction.getSenderAccountNumber());
        transactionResponse.setReceiverAccountNumber(savedTransaction.getReceiverAccountNumber());
        transactionResponse.setAmount(savedTransaction.getAmount());
        transactionResponse.setType(savedTransaction.getType());
        transactionResponse.setStatus(savedTransaction.getStatus());
        transactionResponse.setDescription(savedTransaction.getDescription());
        transactionResponse.setReferenceNumber(savedTransaction.getReferenceNumber());
        transactionResponse.setFailureReason(savedTransaction.getFailureReason());
        transactionResponse.setCreatedAt(savedTransaction.getCreatedAt());
        transactionResponse.setCompletedAt(savedTransaction.getCompletedAt());

        return transactionResponse;

    }

    private void compensateTransaction(Transaction transaction,String reason){
        log.warn("SAGA COMPENSATION - refunding {} amount {}",transaction.getSenderAccountNumber(),transaction.getAmount());

//         Credit money back to sender synchronously

        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(),transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason+ " - SAGA Compensation executed amount refunded at "+ LocalDateTime.now());
        transactionRepository.save(transaction);

//         Publish refund event - Notification service will alert user

        Map<String,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("reason",reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);

        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}",transaction.getAmount(),transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction){
//        Publish fraud detected -> Account Service will be blocked
          Map<String,Object> fraudEvent = new HashMap<>();
          fraudEvent.put("transactionId",transaction.getId());
          fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
          fraudEvent.put("reason", "Wrong OTP entered - transaction cancelled account blocked for security");

//          publish an event basically acoount service will check for this topic and block the account
          kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);

          log.warn("Fraud Detected Published and account will be blocked {} Kindly contact to your back",transaction.getSenderAccountNumber());

//          SAGA COMPENSATION - REFUND SENDER

        compensateTransaction(transaction, "Wrong OTP entered - transaction cancelled account blocked for security");
    }

    private void completeTransaction(Transaction transaction){
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());

        transactionRepository.save(transaction);

        TransactionCompletedEvent transactionCompletedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),transactionCompletedEvent);

        log.info("SAGA COMPLETE - Transaction Complete {} ",transaction.getId());


    }


    public TransactionResponse verifyOtp(String transactionId, String otp){
        log.info("OTP Verification for the transaction {}",transactionId);

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException("Transaction Not Found"+transactionId));

        String otpKey = "verification:otp"+transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            log.warn("OTP expired for Transaction {} ",transactionId);

            compensateTransaction(transaction,"OTP expireed - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }
        if(!storedOtp.equals(otp)){
//            Block the account and refund
            log.warn("Wrong Otp - blocking account and refunding {}",transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction);

            return mapToResponse(transaction);
        }
//            complete the transaction
            log.info("OTP Verified - Completing Transaction {} ",transactionId);
            redisTemplate.delete(otpKey);
            completeTransaction(transaction);
            return mapToResponse(transaction);
    }

    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(()-> new RuntimeException(" Transaction Not Found : "+transactionId));
//

//        idempotency guard
        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not Processing - skipping",transactionId);
            return;
        }
        completeTransaction(transaction);
    }
}


