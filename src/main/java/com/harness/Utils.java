package com.harness;

public class Utils {

    public static void sleepQuietly(long timeInMs) {
        try {
            Thread.sleep(timeInMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
