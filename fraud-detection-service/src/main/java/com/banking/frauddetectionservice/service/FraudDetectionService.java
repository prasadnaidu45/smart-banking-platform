package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    @Value("${fraud.max-transaction-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage}")
    private double maxBalancePercentage;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT = "fraud.clean.check";
    private final RedisTemplate<String,String> redisTemplate;


    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final AccountServiceClient accountServiceClient;
    public void checkTransaction(Map<String,Object> payload){
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // fetch the Balance of the sender

        BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
        log.info("Checking Transaction : Account {}  amount : {} , balance : {}",accountNumber,amount,senderBalance);

        FraudCheckResult result= performFraudChecks(accountNumber,amount,senderBalance);

        if(result.isFraud()){
            log.info("Suspicious Activity Detected - account {} "+ " Reason : {} - requesting OTP Verification ",accountNumber,result.getReason());
            Map<String,Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId",transactionId);
            verificationEvent.put("accountNumber",accountNumber);
            verificationEvent.put("amount",amount);
            verificationEvent.put("reason",result.getReason());

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC,transactionId,verificationEvent);
        }else{
            log.info("Transaction Clean ");
            Map<String,Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId",transactionId);
            transactionCleanEvent.put("isFraud",false);
            transactionCleanEvent.put("reason",null);
            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT,transactionId,transactionCleanEvent);
        }
    }

    /*
      *  we would perform 3 kind of check to flag any transaction as Suspicious
      * 1. velocity check -> how fast the transaction are made
      * 2. average check -> if the with drawl is made above his average transaction or 3 or 5 times greater than the average
      * 3. withdrawing 90% of the user available balance
     */

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal senderBalance) {
        // Pattern 1 : Velocity check
        if(isVelocityExceeded(accountNumber)){
            return new FraudCheckResult(
                    true,
                    "Too Many Transaction in 60 seconds Velocity Limit Exceeded"
            );
        }

        if(isAmountSuspicious(accountNumber,amount)){
            return new FraudCheckResult(
                    true,
                    "Unusual Transaction Amount Exceeds 3 times your average "
            );
        }

        if(senderBalance.compareTo(BigDecimal.ZERO)>0 && isBalanceCheckFailed(senderBalance,amount)){
            return new FraudCheckResult(
                    true,
                    "Transaction Exceeds 90% of amount balance"
            );
        }

        return new FraudCheckResult(false,null);

    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount {} and max Allowed : {} Suspicious : {}",amount,maxAllowed,amount.compareTo(maxAllowed)>0);

        return amount.compareTo(maxAllowed)>0;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String average = "fraud:avg_amount"+accountNumber;
        String avgStr = redisTemplate.opsForValue().get(average);
        if(avgStr==null){
            redisTemplate.opsForValue().set(average,amount.toString());
            return false;
        }
        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

        // update the running average

        BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2),2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(average,newAvg.toString());

        log.info("Amount check : amount : {} threshold :{} suspicious : {}",amount,threshold,amount.compareTo(threshold)>0);

        return amount.compareTo(threshold)>0;

    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity"+accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if(count!=null && count ==1){
            redisTemplate.expire(key, Duration.ofSeconds(60));
        }
        log.info("Velocity check - account {} count {}/{} ",accountNumber,count,maxTransactionPerMinute);

        return count!=null && count > maxTransactionPerMinute;
    }
}
