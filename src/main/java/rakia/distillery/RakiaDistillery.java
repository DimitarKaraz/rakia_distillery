package rakia.distillery;

import rakia.util.Util;
import rakia.workers.Distiller;
import rakia.workers.Gatherer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RakiaDistillery extends Thread{
    List<Gatherer> allGatherers;
    List<Distiller> allDistillers;
    Pot[] allPots;
    LoggerMySQL sqlLogger;
    LoggerFiles fileLogger;
    private AtomicInteger totalLitresDistilled;  //not really enough
    private final int targetLitres = 10;

    public RakiaDistillery() {
        this.allGatherers = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            allGatherers.add(new Gatherer("(g-" + (i+1) +")" , Util.randomAge(), this));
        }
        this.allDistillers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allDistillers.add(new Distiller("(DISTILLER-" + (i+1) +")", Util.randomAge(), this));
        }
        this.allPots = new Pot[5];
        for (int i = 0; i < allPots.length; i++) {
            allPots[i] = new Pot(this);
        }
        this.sqlLogger = new LoggerMySQL(this);
        this.fileLogger = new LoggerFiles(this);
        this.totalLitresDistilled = new AtomicInteger(0);
    }

    @Override
    public void run() {
        startWorking();

        while (keepWorking()) {
            try {
                synchronized (this) {
                    this.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        sqlLogger.printStats();
        fileLogger.exportFiles();
    }

    public void startWorking() {
        this.allGatherers.forEach(gatherer -> gatherer.start());
        this.allDistillers.forEach(distiller -> distiller.start());
        this.sqlLogger.start();
        this.fileLogger.start();
    }

    public void logRakiaBatch(Fruit currentFruit, int litresDistilled, String distillerId) {
        if (currentFruit == null || litresDistilled < 0 || distillerId == null) {
            System.out.println("Logging error. Rakia was lost.");
            return;
        }
        if (this.sqlLogger.insertRakia(currentFruit, litresDistilled, distillerId)) {
            synchronized (this) {
                this.totalLitresDistilled.addAndGet(litresDistilled);
                if (!keepWorking()) {
                    this.notify();
                }
            }
        }
    }

    private boolean keepWorking() {
        return this.totalLitresDistilled.get() < this.targetLitres;
    }

    //TODO: getters here should return copies of collections (immutability)

    public Pot[] getAllPots() {
        return allPots;
    }

    List<Gatherer> getAllGatherers() {
        return allGatherers;
    }

    List<Distiller> getAllDistillers() {
        return allDistillers;
    }

    int getTotalLitresDistilled() {
        return totalLitresDistilled.get();
    }

    public LoggerMySQL getSqlLogger() {
        return sqlLogger;
    }

    public LoggerFiles getFileLogger() {
        return fileLogger;
    }
}
