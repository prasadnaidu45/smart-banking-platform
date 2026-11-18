package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.security.SecureRandom;
import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request){
        log.info("Creating account for : {}" ,request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Account already exist by email :"+request.getEmail());
        }
        Account account = Account.builder()
                .accountHolderName(request.getAccountHolderName())
                .email(request.getEmail())
                .phone(request.getPhone())
                .accountType(request.getAccountType())
                .status(AccountStatus.ACTIVE)
                .balance(request.getInitialDeposit())
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(
                        request.getAccountType() == AccountType.SAVINGS ? new BigDecimal("100000") : new BigDecimal("500000")
                ).build();
        Account saved = accountRepository.save(account);
        log.info("Account Created : {} ",saved.getAccountNumber());
        return mapToResponse(saved);
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setStatus(account.getStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());
        return response;
    }

    private String generateAccountNumber(){

//         do while work until there is new number generated which is not in the database then the loop stops
        String accountNumber;
        do {
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d",number);
        }while(accountRepository.existsByAccountNumber(accountNumber));
        return accountNumber;
    }


    public AccountResponse getAccount(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(()-> new RuntimeException(("Account Not Found")));
        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(()-> new RuntimeException(("Account Not Found")));
        return account.getBalance();
    }

    /*
    Block Account -> called by Fraud Detection Service via Kafka
     */
    public void blockAccount(String accountNumber){
        log.info("Blocking Account : {}",accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(()-> new RuntimeException(("Account Not Found")));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account Blocked {}" , accountNumber);
    }

    /*
    Deduct Balance from the sender account and called by Transaction Service
     */
    public void deductBalance(String accountNumber, BigDecimal amount){
        log.info("Deducting Balance from {} ",accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(()-> new RuntimeException(("Account Not Found")));
        if(account.getStatus()!= AccountStatus.ACTIVE){
            throw new RuntimeException("Account is not Active "+accountNumber);
        }

        if(account.getBalance().compareTo(amount) < 0){
            throw new RuntimeException("InSufficient Fund in the Account "+accountNumber);
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance updated New Balance {} ",account.getBalance());

    }


    /*
    Credit Balance -> called by Transaction Service via Kafka
     */
    public void creditBalance(String accountNumber,BigDecimal amount){
        log.info("Crediting To Account {} ",accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber).orElseThrow(()-> new RuntimeException(("Account Not Found")));
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Amount Credited to Account : {} Updated Balance is  : {} ",accountNumber,account.getBalance());

    }
}
