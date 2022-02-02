package rakia.workers;

import rakia.distillery.RakiaDistillery;

public abstract class Worker extends Thread{
    /*
    *  ALL WORKERS ARE DAEMON THREADS -> they'll work until the target is reached.
    */
    String name;
    int age;
    RakiaDistillery distillery;

    public Worker(String name, int age, RakiaDistillery distillery) {
        this.name = name;
        this.age = age;
        this.distillery = distillery;
        this.setDaemon(true);
    }

    public RakiaDistillery getDistillery() {
        return distillery;
    }

    public String getFullName() {
        return name;
    }

    public int getAge() {
        return age;
    }
}
