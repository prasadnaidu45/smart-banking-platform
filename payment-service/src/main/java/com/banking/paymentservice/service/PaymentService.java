package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC  = "payment.failed";


    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;
    /*
    Create RazorPay payment Order
    * first need to create order in razorPay
    *  save the payment record in the db
    * return order details to the front end
    * front end pays shows razor pay checkout page
    * user page
    * razor pay call webhook

     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment Order for account : {} and amount {} ",request.getAccountNumber(),request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        // Convert Amount
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest =  new JSONObject();
        orderRequest.put("amount",convertedAmount);
        orderRequest.put("currency","INR");
        orderRequest.put("receipt","rcpt"+ System.currentTimeMillis()+UUID.randomUUID().toString().replace("-","").substring(0,10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay Order Created {} ",razorpayOrder.get("id").toString());


//        Save payment record in the database

        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "INR",
                "CREATED",
                keyId
        );
    }

    public void handleWebhook(Map<String,Object> payload){
        log.info("Received RazorPay webhook {} ",payload.get("event"));

        String event = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }


    }
    private void handlePaymentSuccess(Map<String,Object> payload){
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("orderId");
            String paymentId = (String) paymentData.get("paymentId");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException("Payment Not Found"));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);

            paymentRepository.save(payment);

//            Publish payment completed event

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",payment.getRazorpayPaymentId());

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(),event);

            log.info("Payment Completed {}",payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment Success {}",e.getMessage());

        }
    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String,Object> entity = (Map<String,Object>)payload.get("payload");

        Map<String,Object> paymentWrapper = (Map<String,Object>)entity.get("payment");

        return (Map<String,Object>)paymentWrapper.get("entity");
    }

    private void handlePaymentFailure(Map<String,Object> payload){
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("orderId");
            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()-> new RuntimeException("Payment Not Found"));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment Failed via Razorpay");
            paymentRepository.save(payment);

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("reason","Payment Failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);

            log.warn("Payment Failed {}",payment.getId());

        } catch (Exception e) {
            log.error("Error Handling payment Failure {} ",e.getMessage());
        }
    }

}
