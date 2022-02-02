package rakia.distillery;

import rakia.workers.Distiller;
import rakia.workers.Gatherer;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Pot {
    private final RakiaDistillery myDistillery;

    private Fruit currentFruit;
    private static final int minKgToDistill = 10;
    private final AtomicInteger kg;
    private final AtomicBoolean awaitingDistillation;
    private final AtomicBoolean isOccupiedByDistiller;

    public Pot(RakiaDistillery myDistillery) {
        this.myDistillery = myDistillery;
        this.kg = new AtomicInteger(0);
        this.awaitingDistillation = new AtomicBoolean(false);
        this.isOccupiedByDistiller = new AtomicBoolean(false);
    }

    public Fruit getCurrentFruit() {
        return currentFruit;
    }

    public int getKg() {
        return kg.get();
    }

    public boolean getAwaitingDistillation() {
        return awaitingDistillation.get();
    }

    public void setAwaitingDistillation(boolean awaitingDistillation) {
        this.awaitingDistillation.set(awaitingDistillation);
    }

    public boolean isOccupiedByDistiller() {
        return isOccupiedByDistiller.get();
    }

    public void setOccupiedByDistiller(boolean occupiedByDistiller) {
        this.isOccupiedByDistiller.set(occupiedByDistiller);
    }

    public void setCurrentFruit(Fruit currentFruit) {
        this.currentFruit = currentFruit;
    }


    public boolean receiveFruit(Fruit frutto) {
        if (this.getCurrentFruit() != frutto && this.getCurrentFruit() != null) {
            System.out.println("Fruit type mismatch.");
            return false;
        }
        synchronized (this) {
            this.addKgOfFruit(Gatherer.getAMOUNT());     //Pot.class.notifyAll()
            System.out.println(Thread.currentThread().getName() + " added " +  Gatherer.getAMOUNT() + " kg " +
                    this.currentFruit + " to " + this + ".  Total kg = " + this.getKg());
            return true;
        }
    }

    private void addKgOfFruit(int amount) {
        if (amount <= 0) {
            return;
        }
        //add and check amount -> then check if it's already awaiting distillation
        if (this.kg.addAndGet(amount) >= minKgToDistill && !this.awaitingDistillation.get()) {
            this.awaitingDistillation.set(true);
            synchronized (Pot.class) {
                Pot.class.notifyAll();
            }
        }
    }

    public boolean getDistilledIntoRakia() throws InterruptedException {
        synchronized (this) {
            System.out.println(Thread.currentThread().getName() + " STARTS TO DISTILL RAKIA IN " + this);
            Thread.sleep(Distiller.getDistillationTime());

            int litresDistilled = Distiller.randomLitres(this.getKg());

            synchronized (myDistillery) {
                myDistillery.logRakiaBatch(this.getCurrentFruit(), litresDistilled, String.valueOf(Thread.currentThread().getId()));     //get Logger -> make INSERT query to database
                System.out.println(Thread.currentThread().getName() + " distilled " + litresDistilled + " L of rakia from "
                        + this + ".   TOTAL RAKIA (L) = " + myDistillery.getTotalLitresDistilled());
            }

            this.emptyPot(); //Pot.class.notifyAll()
            return true;
        }
    }

    private void emptyPot() {
        this.kg.set(0);
        this.awaitingDistillation.set(false);
        this.isOccupiedByDistiller.set(false);
    }

    @Override
    public String toString() {
        return "Pot_" + Integer.toHexString(this.hashCode());
    }


}
