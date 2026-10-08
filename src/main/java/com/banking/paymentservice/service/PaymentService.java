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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id:}")
    private String keyId;

    @Value("${razorpay.key-secret:}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) {
        log.info("Creating payment order for account :{} amount : {}",
                request.getAccountNumber(), request.getAmount());

        if (keySecret == null || keySecret.isBlank()) {
            return createDemoOrder(request);
        }

        try {
            RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

            int convertedAmount = request.getAmount()
                    .multiply(BigDecimal.valueOf(100))
                    .intValue();
            JSONObject orderRequest = new JSONObject();
            orderRequest.put("amount", convertedAmount);
            orderRequest.put("currency", "INR");
            orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                    .replace("-", "")
                    .substring(0, 10));

            Order razorpayOrder = razorpayClient.orders.create(orderRequest);

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
        } catch (RazorpayException e) {
            log.error("Failed to create Razorpay order: {}", e.getMessage());
            throw new RuntimeException("Failed to create payment order", e);
        }
    }

    public PaymentOrderResponse simulateSuccess(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));
        if (payment.getStatus() == PaymentStatus.COMPLETED) {
            return toResponse(payment);
        }
        payment.setStatus(PaymentStatus.COMPLETED);
        payment.setRazorpayPaymentId("pay_demo_" + UUID.randomUUID().toString().substring(0, 8));
        paymentRepository.save(payment);
        publishCompleted(payment);
        return toResponse(payment);
    }

    public List<Payment> getPaymentsByAccount(String accountNumber) {
        return paymentRepository.findByAccountNumberOrderByCreatedAtDesc(accountNumber);
    }

    public Payment getPayment(String paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));
    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook: {}", payload.get("event"));
        String event = (String) payload.get("event");
        if ("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private PaymentOrderResponse createDemoOrder(CreatePaymentRequest request) {
        Payment payment = new Payment();
        payment.setRazorpayOrderId("order_demo_" + UUID.randomUUID().toString().substring(0, 8));
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription() == null ? "Demo deposit" : request.getDescription());
        Payment saved = paymentRepository.save(payment);
        log.info("Created demo payment order (no Razorpay secret configured): {}", saved.getId());
        return toResponse(saved);
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found for order: " + orderId));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);
            publishCompleted(payment);
        } catch (Exception e) {
            log.error("Error handling payment success: {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found for order: " + orderId));
            payment.setStatus(PaymentStatus.FAILED);
            payment.setRazorpayPaymentId(paymentId);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);
            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);
            log.warn("Payment failed :{} ", payment.getId());
        } catch (Exception e) {
            log.error("Error handling payment failure: {}", e.getMessage());
        }
    }

    private void publishCompleted(Payment payment) {
        Map<String, Object> event = new HashMap<>();
        event.put("paymentId", payment.getId());
        event.put("accountNumber", payment.getAccountNumber());
        event.put("amount", payment.getAmount());
        event.put("razorpayPaymentId", payment.getRazorpayPaymentId());
        kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
        log.info("payment completed: {}", payment.getId());
    }

    private PaymentOrderResponse toResponse(Payment payment) {
        return new PaymentOrderResponse(
                payment.getId(),
                payment.getRazorpayOrderId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus().name(),
                keyId
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");
        return (Map<String, Object>) paymentWrapper.get("entity");
    }
}
