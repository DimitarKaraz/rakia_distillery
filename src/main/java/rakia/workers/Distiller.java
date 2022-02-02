package rakia.workers;

import rakia.distillery.Pot;
import rakia.distillery.RakiaDistillery;

import java.util.concurrent.ThreadLocalRandom;

public class Distiller extends Worker {
    //DAEMON THREAD
    private static final int DISTILLATION_TIME = 1000;  //milliseconds

    public Distiller(String name, int age, RakiaDistillery distillery) {
        super(name, age, distillery);
    }

    @Override
    public void run() {
        this.setName(this.name);
        Pot myPot;
        while (true) {
            try {
                synchronized (Pot.class) {
                    while ((myPot = findReadyPot()) == null) {
                        System.out.println(this.name + " is waiting for Ready pots...");
                        Pot.class.wait();
                    }
                }
                myPot.getDistilledIntoRakia();
            } catch (InterruptedException e) {
                System.out.println(this.name + " was interrupted while gathering!");
            }
        }
    }

    public static int randomLitres(int kg) {
        return ThreadLocalRandom.current().nextInt(kg) + 1;
    }

    private Pot findReadyPot() {    //this runs inside synchronized(Pot.class){...}
        Pot[] pots = this.distillery.getAllPots();
        for (int i = 0; i < pots.length; i++) {
            if (pots[i].getAwaitingDistillation() && !pots[i].isOccupiedByDistiller()) {
                pots[i].setAwaitingDistillation(false);
                pots[i].setOccupiedByDistiller(true);
                return pots[i];
            }
        }
        return null;
    }

    public static int getDistillationTime() {
        return DISTILLATION_TIME;
    }
}
