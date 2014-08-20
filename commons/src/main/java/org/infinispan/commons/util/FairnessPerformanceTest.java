package org.infinispan.commons.util;

import org.infinispan.commons.util.concurrent.NonReentrantWriterPreferredReadWriteLock;

import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author slukjanov aka Frostman
 */
public class FairnessPerformanceTest {
    private static final AtomicInteger WORKERS_COUNT = new AtomicInteger();
    private static final NumberFormat NF = getNumberFormat();

    private final Lock lock;
    private final int maxAcquires;

    private AtomicInteger totalAcquires = new AtomicInteger();

    public static void main(String[] args) {
        boolean badArgs = true, fair = false;
        int threads = 0, maxAcquires = 0;
        if (args.length == 3) {
            try {
                threads = Integer.parseInt(args[0]);
                maxAcquires = Integer.parseInt(args[1]);
                fair = Boolean.parseBoolean(args[2]);
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
            badArgs = false;
        }
        if (badArgs) {
            System.err.println("Mandatory args: THREADS MAX_ACQUIRES FAIRNESS");
            System.exit(1);
        }

        new FairnessPerformanceTest(threads, maxAcquires, fair);
    }

    public FairnessPerformanceTest(int threads, int maxAcquires, boolean fair) {
//        lock = new ReentrantReadWriteLock(fair).readLock();
        lock = new NonReentrantWriterPreferredReadWriteLock().readLock();
        this.maxAcquires = maxAcquires;

        System.out.println("FairnessPerformanceTest: " + (fair ? "" : "non-") + "fair lock");

        System.out.println("Warming up");
        test(threads * 2, true);

        System.out.println("Start testing:");
        test(threads, false);
    }

    private void test(int threads, boolean silent) {
        totalAcquires.set(0);
        WORKERS_COUNT.set(0);
        long start = System.nanoTime();
        Worker[] workers = new Worker[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Worker();
            workers[i].start();
        }

        List<Double> deviations = new LinkedList<Double>();
        for (Worker worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                throw new RuntimeException("Worker#join interrupted, id: " + worker.id, e);
            }

            if (!silent) {
                double percents = 1.0 * worker.acquires / totalAcquires.get() * 100;
                double deviation = Math.abs(100.0 / threads - percents);
                deviations.add(deviation);
                System.out.println("Worker#" + worker.id + " acquires: " + NF.format(percents) + "%"
                        + " deviation: " + NF.format(deviation) + "%");
            }
        }

        if (!silent) {
            double maxDeviation = 0;
            double minDeviation = 100;
            double averageDeviation = 0;
            for (Double deviation : deviations) {
                maxDeviation = Math.max(maxDeviation, deviation);
                minDeviation = Math.min(minDeviation, deviation);
                averageDeviation += deviation;
            }
            averageDeviation /= threads;
            System.out.println("\nDeviation:\n\tmax: " + NF.format(maxDeviation) + "%\n\tavg: "
                    + NF.format(averageDeviation) + "%\n\tmin: " + NF.format(minDeviation) + "%");
            System.out.println("Takes " + (System.nanoTime() - start) / 1000000 + "ms");
        }
    }

    private class Worker extends Thread {
        private int id = WORKERS_COUNT.getAndIncrement();
        private int acquires = 0;

        @Override
        public void run() {
            while (true) {
                lock.lock();
                try {
                    acquires++;
                    if (totalAcquires.incrementAndGet() > maxAcquires) {
                        return;
                    }

                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private static NumberFormat getNumberFormat() {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumFractionDigits(4);
        nf.setMaximumFractionDigits(4);
        nf.setMaximumIntegerDigits(2);

        return nf;
    }
}
