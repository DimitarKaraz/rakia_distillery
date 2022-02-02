package rakia.util;

import java.util.concurrent.ThreadLocalRandom;

public class Util {
    private static String[] names = new String[]{"Boris Johnson", "Mariah Cary", "Peter Pan", "Jimmy Fallon", "Barack Obama", "Donald Trump", "Boiko Borisov", "James Bond", "Michael Jordan", "Boris Dali", "Salvador Dali", "Penelope Cruz"};

    public static String randomName() {
        return names[ThreadLocalRandom.current().nextInt(names.length)];
    }

    public static int randomAge() {
        return ThreadLocalRandom.current().nextInt(18, 60);
    }
}
