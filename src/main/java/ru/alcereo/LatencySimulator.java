package ru.alcereo;

/**
 * Created by alcereo on 23.09.17.
 */
public class LatencySimulator {

    public static void sleep(int milliseconds) {

        long startTime = System.currentTimeMillis();

        while(System.currentTimeMillis()-startTime<milliseconds){
            int b=0;
            for (int j = 0; j < 10000; j++) {
                b=b%(j+1);
            }
        }

    }
}
