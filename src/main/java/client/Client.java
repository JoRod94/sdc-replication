package client;

import bank.Bank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by frm on 29/04/16.
 */
public class Client implements Runnable {
    private static int NR_REQUESTS = 500;
    private static int NR_THREADS = 32;
    private static int NR_SEEDS = 50;
    private static ReadWriteLock lock = new ReentrantReadWriteLock();
    private static CountDownLatch startSignal = new CountDownLatch(1);
    private static CountDownLatch doneSignal = new CountDownLatch(NR_THREADS);

    private Bank bank;
    private Map<String, Integer> accounts;
    private int id;

    public Client(int id, Map<String, Integer> m) throws IOException {
        this.bank = new BankStub();
        this.accounts = m;
        this.id = id;
    }

    private Integer readOp(Function<Map<String, Integer>, Integer> f) {
        lock.readLock().lock();
        Integer i = f.apply(accounts);
        lock.readLock().unlock();
        return i;
    }

    private void writeOp(Consumer<Map<String, Integer>> f) {
        lock.writeLock().lock();
        f.accept(accounts);
        lock.writeLock().unlock();
    }

    private String getRandomAccount() {
        // account ids are one-based, random is zero-based and outer-bound exclusive
        // we add one to avoid issues with this
        return Integer.toString(new Random().nextInt(readOp((a) -> a.size())) + 1);
    }

    private void createAccount() {
        String newId = bank.create();
        writeOp((a) -> a.put(newId, 0));
    }

    private Integer requestBalance() {
        return bank.balance(getRandomAccount());
    }

    private String requestLogs() {
        String account = getRandomAccount();
        int n = new Random().nextInt(10) + 1; // interval: [1, 10]
        return bank.latest(account, n);
    }

    private void makeMovement() {
        int amount = new Random().nextInt(1001); // Random#nextInt is exclusive for the outer bound
        String account = getRandomAccount();
        if(bank.movement(account, amount)) {
            try {
                writeOp((a) -> a.put(account, a.get(account) + amount));
            } catch(NullPointerException e) {
                System.out.println(new StringBuilder()
                        .append("FOUND INEXISTING ID: ").append(account)
                        .append("\nDIAGNOSING...")
                        .append("\nSIZE: ").append(accounts.size())
                        .append("\n ").append(account).append(" EXISTS? ").append(accounts.get(account))
                        .append("\n TOTAL ORDER MULTICAST ERROR. IGNORING...")
                        .toString());

                lock.writeLock().unlock();
            }
        }
    }

    private void makeTransfer() {
        int amount = new Random().nextInt(1001); // [0, 1000]
        final String origin = getRandomAccount();
        String dest;

        do {
            dest = getRandomAccount();
        } while(dest == origin); // ensures we are making a transfer between diff accounts

        final String destination = dest; // we need this to be in a final variable

        if(bank.transfer(origin, destination, amount)) {
            writeOp((a) -> {
                a.put(origin, a.get(origin) - amount);
                a.put(destination, a.get(destination) + amount);
            });
        }
    }

    private void seed(int nrSeeds) {
        for(int i = 0; i < nrSeeds; i++)
            createAccount();
    }

    private void randomRequests(int nrRequests) {
        ThreadLocalRandom op = ThreadLocalRandom.current();

        for(int i = 0; i < nrRequests; i++) {
            int nextOp = op.nextInt(1, 4);
            switch(nextOp) {
                case 1: // CREATE
                    createAccount();
                    break;
                case 2: // BALANCE
                    requestBalance();
                    break;
                case 3: // MOVEMENT
                    makeMovement();
                    break;
                case 4: // TRANSFER
                    makeTransfer();
                    break;
                case 5: // LOG MOVEMENTS
                    requestLogs();
                    break;
            }
        }
    }

    @Override
    public void run() {
        try {
            startSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        seed(NR_SEEDS);
        randomRequests(NR_REQUESTS);
        doneSignal.countDown();
        System.out.println("[END] Thread " + id);
   }

    private static int verify(Bank b, Map<String, Integer> m) {
        int errors = 0;
        for(Map.Entry<String, Integer> pair : m.entrySet()) {
            int balance = b.balance(pair.getKey());
            if(pair.getValue() != balance) {
                errors++;
                System.out.println(
                        new StringBuilder()
                            .append("### VALUE MISMATCH:")
                            .append("\t\tEXPECTED: ")
                            .append(pair.getValue())
                            .append(" GOT: ")
                            .append(balance)
                            .append(" - ACCOUNT: ")
                            .append(pair.getKey())
                            .toString()
                );
            }
        }

        return errors;
    }

    private static void printResults(long start, long end, Map<String, Integer> sharedMap) throws IOException {
        long time = end - start;

        long requests = NR_THREADS * (NR_REQUESTS + NR_SEEDS);

        System.out.println(
                new StringBuilder()
                    .append("----- FINISHED TESTING -----")
                    .append("\n----- VERIFYING DATA CONSISTENCY -----")
                    .toString()
        );

        int errors = verify(new BankStub(), sharedMap);

        System.out.println(
                new StringBuilder()
                    .append("----- FINISHED VERIFYING DATA CONSISTENCY -----")
                    .append("\n")
                    .append(errors)
                    .append(" ERRORS FOUND")
                    .append("\nTOTAL TIME: ")
                    .append(time)
                    .append("ms\nNR REQUESTS: ")
                    .append(requests)
                    .append("\nTHROUGHPUT: ")
                    .append(((double)requests)/((double)time / 1000.0))
                    .append(" req/s\nLATENCY: ")
                    .append(((double)time/1000.0)/(double)requests)
                    .append(" s/req")
                    .toString()
        );
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, Integer> m = new HashMap<>();

        for(int i = 0; i < NR_THREADS; i++)
            new Thread(new Client(i + 1, m)).start();

        startSignal.countDown(); // start testing simultaneously
        long start = System.currentTimeMillis();
        doneSignal.await(); // wait for threads to end
        long end = System.currentTimeMillis();

        printResults(start, end, m);
        System.exit(0);
    }
}
