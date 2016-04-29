package client;

import bank.Bank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by frm on 29/04/16.
 */
public class Client extends Thread {
    private static ReadWriteLock lock = new ReentrantReadWriteLock();
    private static int NR_REQUESTS = 1000;
    private static int NR_THREADS = 8;
    private static int NR_SEEDS = 10;

    private Bank bank;
    private Map<String, Integer> accounts;

    public Client(Map<String, Integer> m) throws IOException {
        this.bank = new BankStub();
        this.accounts = m;
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
        return Integer.toString( new Random().nextInt(readOp((a) -> a.size())) + 1 );
    }

    private void createAccount() {
        String newId = bank.create();
        writeOp((a) -> a.put(newId, 0));
    }

    private Integer requestBalance() {
        return bank.balance(getRandomAccount());
    }

    private void makeMovement() {
        int amount = new Random().nextInt(1001); // Random#nextInt is exclusive for the outer bound
        String account = getRandomAccount();
        if( bank.movement(account, amount) )
            writeOp((a) -> a.put( account, a.get(account) + amount));
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
            }
        }

    }

    private static void verify(Bank b, Map<String, Integer> m) {
        for(Map.Entry<String, Integer> pair : m.entrySet()) {
            int balance = b.balance(pair.getKey());
            if(pair.getValue() != balance)
                System.out.println("EXPECTED: " + pair.getValue() + " AND GOT: " + balance);
        }
    }

    @Override
    public void run() {
        seed(NR_SEEDS);
        randomRequests(NR_REQUESTS);
   }

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, Integer> m = new HashMap<>();
        Thread[] threads = new Thread[NR_THREADS];

        for(int i = 0; i < NR_THREADS; i++) {
            threads[i] = new Client(m);
            threads[i].start();
        }

        for(Thread t : threads)
            t.join();

        System.out.println("VERIFYING...");
        verify(new BankStub(), m);
        System.out.println("DONE VERIFYING");
    }
}
