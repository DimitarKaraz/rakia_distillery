package rakia.workers;

import rakia.distillery.Fruit;
import rakia.distillery.Pot;
import rakia.distillery.RakiaDistillery;

import java.util.concurrent.ThreadLocalRandom;

public class Gatherer extends rakia.workers.Worker {
    private Fruit currentFruit;
    private static final int AMOUNT = 1; //1 kg
    private static final int GATHERING_TIME = 200;  //milliseconds

    public Gatherer(String name, int age, RakiaDistillery distillery) {
        super(name, age, distillery);
    }

    @Override
    public void run() {
        this.setName(this.name);
        Pot currentPot;
        while (true) {
            try {
                setRandomFruit();
                Thread.sleep(GATHERING_TIME);
                if ((currentPot = findAvailablePot()) == null) {
                    System.out.println(this.name + " FOUND NO AVAILABLE POTS. HE WILL TRY AGAIN AFTER 500 MS...");
                    Thread.sleep(500);
                    continue;
                }
                currentPot.receiveFruit(this.currentFruit);
            } catch (InterruptedException e) {
                System.out.println(this.name + " was interrupted while gathering!");
            }
        }
    }

    public static int getAMOUNT() {
        return AMOUNT;
    }

    private void setRandomFruit() {
        this.currentFruit = Fruit.values()[ThreadLocalRandom.current().nextInt(Fruit.values().length)];
    }


    private Pot findAvailablePot() {    //this runs inside synchronized(Pot.class){...}
        Pot[] pots = this.distillery.getAllPots();
        for (int i = 0; i < pots.length; i++) {
            if ((pots[i].getCurrentFruit() == this.currentFruit
                    || pots[i].getCurrentFruit() == null)) {
                pots[i].setCurrentFruit(this.currentFruit);
                return pots[i];
            }
        }
        return null;
    }

}
