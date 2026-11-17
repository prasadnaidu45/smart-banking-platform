# Smart Banking Platform

A Scalable digital banking and UPI payment system built using microservices architecture.

# Digital Banking System

A microservices-based banking backend built with **Java 17, Spring Boot, Spring Cloud Gateway, Apache Kafka, Redis and MySQL**.
It supports account creation, money transfers using an event-driven **saga**, rule-based **fraud detection** with **OTP step-up verification**, payment orders via **Razorpay**, and notifications.

---

## Table of Contents
1. [Architecture](#architecture)
2. [Services](#services)
3. [Tech Stack](#tech-stack)
4. [Kafka Topics](#kafka-topics)
5. [Transfer Flow (Saga)](#transfer-flow-saga)
6. [Fraud Detection Rules](#fraud-detection-rules)
7. [Payment Flow (Razorpay)](#payment-flow-razorpay)
8. [API Reference](#api-reference)
9. [Running Locally](#running-locally)
10. [Configuration Notes](#configuration-notes)

---

## Architecture

```
                         ┌──────────────────────┐
        Client/Postman → │ API Gateway  :8080   │  (routing + Redis rate limiting)
                         └─────────┬────────────┘
             ┌─────────────────────┼──────────────────────┐
             ▼                     ▼                      ▼
   ┌──────────────────┐  ┌────────────────────┐  ┌──────────────────┐
   │ Account Service  │  │ Transaction Service│  │ Payment Service  │
   │      :8081       │  │       :8082        │  │      :8083       │
   │   (account_db)   │  │  (transaction_db)  │  │   (payment_db)   │
   └────────▲─────────┘  └───┬────────────▲───┘  └────────┬─────────┘
            │  Feign (REST)  │            │               │
            └────────────────┘            │               │
                                          │ Kafka         │ Kafka
                     ┌────────────────────┴───────────────┴──────────┐
                     │                 Apache Kafka                   │
                     └──────▲─────────────────────────▲───────────────┘
                            │                         │
                 ┌──────────┴─────────┐    ┌──────────┴──────────┐
                 │ Fraud Detection    │    │ Notification Service│
                 │ Service  :8084     │    │       :8085         │
                 │ (Redis counters)   │    │ (alerts / OTP)      │
                 └────────────────────┘    └─────────────────────┘
```

- Services communicate **synchronously** over REST (OpenFeign) where an immediate answer is needed (deduct balance, fetch balance) and **asynchronously** over Kafka for everything else.
- Each data-owning service has its own MySQL database (database-per-service).
- Redis is used for rate limiting (gateway), fraud counters (fraud service) and OTP storage (transaction service).

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| **api-gateway** | 8080 | Single entry point. Routes `/api/v1/accounts/**`, `/api/v1/transactions/**`, `/api/v1/payments/**` to the right service. Applies a Redis-backed `RequestRateLimiter` per route. |
| **account-service** | 8081 | Creates accounts, returns account/balance, deducts and credits balances, blocks accounts. Listens to Kafka to credit the receiver and to block fraudulent accounts. |
| **transaction-service** | 8082 | Orchestrates the transfer saga. Stores transactions and their status, generates and verifies OTPs, performs compensation (refund). |
| **payment-service** | 8083 | Creates Razorpay orders, stores payment records, handles Razorpay webhooks and publishes payment results to Kafka. |
| **fraud-detection-service** | 8084 | Consumes new transactions, runs fraud rules, and publishes either a "clean" result or a "verification required" event. |
| **notification-service** | 8085 | Consumes events and sends alerts (OTP, debit/credit, fraud, refund, payment success/failure). Alerts are currently logged. |

### Account statuses and data
- `Account`: account number, holder name, email, phone, account type (e.g. `SAVINGS`), status (`ACTIVE`/blocked), balance, daily transaction limit.
- `Transaction` statuses: `PROCESSING` → `PENDING_VERIFICATION` → `COMPLETED` or `FLAGGED` (refunded).

---

## Tech Stack

- **Java 17**, **Spring Boot** (Web, Validation, Data JPA, Actuator)
- **Spring Cloud Gateway (WebFlux)** — routing and rate limiting
- **Spring Cloud OpenFeign** — service-to-service REST calls
- **Apache Kafka** (+ ZooKeeper) — event bus (JSON serialization)
- **Redis** — rate limiter, fraud counters, OTP store with TTL
- **MySQL 8.4** — one database per service
- **Razorpay Java SDK** — payment orders
- **Docker Compose** — Redis, ZooKeeper, Kafka, MySQL
- **Lombok**, **Maven**

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `transaction.initiated` | transaction-service | fraud-detection-service | A transfer was started; run fraud checks |
| `fraud.clean.check` | fraud-detection-service | transaction-service | Transaction looks clean; complete it |
| `verification.required` | fraud-detection-service | transaction-service | Transaction is suspicious; require OTP |
| `transaction.otp.generated` | transaction-service | notification-service | Send the OTP to the user |
| `transaction.completed` | transaction-service | account-service, notification-service | Credit receiver; send debit/credit alerts |
| `transaction.refunded` | transaction-service | notification-service | Notify the sender of a refund |
| `fraud.detected` | transaction-service | account-service, notification-service | Block the sender's account; notify |
| `payment.completed` | payment-service | notification-service | Razorpay payment captured |
| `payment.failed` | payment-service | notification-service | Razorpay payment failed |

---

## Transfer Flow (Saga)

A transfer touches two accounts owned by another service, so it is implemented as a **choreographed saga with a compensating action** instead of a distributed transaction.

```
Client ──POST /api/v1/transactions/transfer──▶ Transaction Service
```

**Step 1 – Initiate (synchronous)**
1. Transaction service calls Account service (`PUT /accounts/{sender}/deduct?amount=…`) via Feign. The sender is debited immediately; insufficient balance or a blocked account fails the request here.
2. A `Transaction` is saved with status `PROCESSING` and a random reference number.
3. A `transaction.initiated` event is published. The API returns right away with status `PROCESSING`.

**Step 2 – Fraud check (asynchronous)**
4. Fraud detection service consumes the event, fetches the sender balance from Account service (Feign), and runs the [fraud rules](#fraud-detection-rules).

**Step 3a – Clean path**
5. Fraud service publishes `fraud.clean.check`.
6. Transaction service marks the transaction `COMPLETED` (only if it is still `PROCESSING` – an idempotency guard) and publishes `transaction.completed`.
7. Account service consumes `transaction.completed` and **credits the receiver**. Notification service sends the debit and credit alerts.

**Step 3b – Suspicious path (OTP)**
5. Fraud service publishes `verification.required` with the reason.
6. Transaction service generates a 6-digit OTP, stores it in Redis (`verification:otp<transactionId>`, **5 minute TTL**), sets the status to `PENDING_VERIFICATION` and publishes `transaction.otp.generated`. Notification service delivers the OTP.
7. The user calls `POST /api/v1/transactions/{id}/verify` with the OTP:
   - **Correct OTP** → transaction completes exactly as in the clean path (receiver is credited).
   - **Wrong OTP** → `fraud.detected` is published (sender's account is **blocked**) and the sender is **refunded** (compensation); status becomes `FLAGGED`.
   - **Expired OTP** → sender is **refunded**; status becomes `FLAGGED`.

**Compensation:** the refund credits the sender back synchronously through Account service and publishes `transaction.refunded` so the user is notified.

### State diagram
```
            ┌────────────┐  clean   ┌───────────┐
 transfer → │ PROCESSING │─────────▶│ COMPLETED │
            └─────┬──────┘          └───────────┘
                  │ suspicious            ▲
                  ▼                       │ correct OTP
        ┌────────────────────┐────────────┘
        │ PENDING_VERIFICATION│
        └─────────┬──────────┘
                  │ wrong / expired OTP
                  ▼
             ┌─────────┐
             │ FLAGGED │  (sender refunded; account blocked on wrong OTP)
             └─────────┘
```

---

## Fraud Detection Rules

Evaluated in order by `FraudDetectionService`; the first rule that matches sends the transaction to OTP verification. Thresholds live in the fraud service's `application.yaml`.

| Rule | Logic | Config |
|---|---|---|
| **Velocity** | Redis counter per sender (`fraud:velocity<account>`, 60 s TTL). Flags if more than N transfers occur in a minute. | `fraud.max-transaction-per-minute` (5) |
| **Unusual amount** | Keeps a running average of the sender's amounts in Redis. Flags if the amount exceeds `average × multiplier`. The first transfer is never flagged. | `fraud.suspicious-amount-multiplier` (5) |
| **Balance percentage** | Flags if the amount is more than a fraction of the sender's balance. | `fraud.max-balance-percentage` (0.90) |

---

## Payment Flow (Razorpay)

1. Client calls `POST /api/v1/payments/create-order` with `accountNumber`, `amount`, `description`.
2. Payment service creates a Razorpay order (amount converted to paise, currency INR), saves a `Payment` as `CREATED`, and returns the order id and key id to the client.
3. The client completes payment on Razorpay's checkout.
4. Razorpay calls `POST /api/v1/payments/webhook`:
   - `payment.captured` → payment `COMPLETED`, publishes `payment.completed`.
   - `payment.failed` → payment `FAILED`, publishes `payment.failed`.
5. Notification service alerts the user.

---

## API Reference

All requests go through the gateway at `http://localhost:8080`.

### Accounts
| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/accounts` | Create an account |
| GET | `/api/v1/accounts/{accountNumber}` | Get account details |
| GET | `/api/v1/accounts/{accountNumber}/balance` | Get balance |
| PUT | `/api/v1/accounts/{accountNumber}/deduct?amount=` | Deduct balance (used by the saga) |
| PUT | `/api/v1/accounts/{accountNumber}/credit?amount=` | Credit balance (used by the saga) |

Create account body:
```json
{
  "accountHolderName": "Test User",
  "email": "test@gmail.com",
  "phone": "9999999999",
  "accountType": "SAVINGS",
  "initialDeposit": 10000
}
```

### Transactions
| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/transactions/transfer` | Start a transfer |
| GET | `/api/v1/transactions/{transactionId}` | Get a transaction |
| GET | `/api/v1/transactions/account/{accountNumber}` | Transactions sent from an account |
| POST | `/api/v1/transactions/{transactionId}/verify` | Submit the OTP for a pending transaction |

Transfer body:
```json
{
  "senderAccountNumber": "333497251274",
  "receiverAccountNumber": "074763196587",
  "amount": 100,
  "description": "rent"
}
```

### Payments
| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/payments/create-order` | Create a Razorpay order |
| POST | `/api/v1/payments/webhook` | Razorpay webhook receiver |

---

## Running Locally

**Prerequisites:** Java 17, Maven (or the included `mvnw`), Docker.

1. **Start infrastructure** (Redis, ZooKeeper, Kafka, MySQL):
   ```bash
   docker compose up -d
   ```
2. **Start each service** (from its folder, or from your IDE), in any order:
   ```bash
   cd account-service && ./mvnw spring-boot:run
   cd transaction-service && ./mvnw spring-boot:run
   cd fraud-detection-service && ./mvnw spring-boot:run
   cd notification-service && ./mvnw spring-boot:run
   cd payment-service && ./mvnw spring-boot:run
   cd api-gateway && ./mvnw spring-boot:run
   ```
3. **Try it:** create two accounts, then transfer between them and check the receiver's balance.

   ```bash
   curl -X POST localhost:8080/api/v1/accounts -H 'Content-Type: application/json' \
     -d '{"accountHolderName":"Alice","email":"a@x.com","phone":"9999999999","accountType":"SAVINGS","initialDeposit":10000}'
   ```

   The OTP for a flagged transfer is printed in the notification service logs.

---

## Configuration Notes

- Databases (`account_db`, `transaction_db`, `payment_db`) are created automatically on first start (`createDatabaseIfNotExists`) and tables are created by Hibernate (`ddl-auto: update`).
- The service `application.yaml` files use the local MySQL root credentials from `docker-compose.yml`. These are **local development values only**.
- Razorpay credentials in `payment-service/src/main/resources/application.yaml` are placeholders. Put real test keys there (or in environment variables) to create orders. **Do not commit real keys.**
- The gateway's route configuration uses the Spring Cloud Gateway 2025.x property prefix `spring.cloud.gateway.server.webflux.routes`.
