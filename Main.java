import javax.swing.*;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static final int SIZE = 100000;
    public static final int THREADS = 5;
    public static final int ITEMS_PER_THREAD = SIZE/THREADS;

    public static double f(double x){
        return x*x*5+53*x+x*Math.cos(x)*4;
    }

    public static Thread taskThread(int n, int[] schedule, List<Double> x, double[][] S){
        return new Thread(()-> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            if (n == 0) start++;
            for (int i = start; i < finish; i++) {
                if (i % 2 == 0)
                    S[n][1] += f(x.get(i));
                else
                    S[n][0] += f(x.get(i));
            }
        });
    }

    public static void SimpsonLinear(double h, List<Double> x)
    {
        double S1 = 0, S2 = 0;
        var pStart = System.nanoTime();
        for (int i = 1; i < SIZE; ++i)
        {
            if (i % 2 == 0)
                S2 += f(x.get(i));
            else
                S1 += f(x.get(i));
        }
        double result = h / 3.0 * (f(x.getFirst()) + f(x.getLast()) + 4 * S1 + 2 * S2);
        var pFinish = System.nanoTime();
        System.out.println("linear result:");
        System.out.println(result);
        System.out.println("linear time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void SimpsonP(double h, List<Double> x) throws InterruptedException {
        double[][] SRes = new double[THREADS][2];
        double S1 = 0, S2 = 0;
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }
        var pStart = System.nanoTime();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, x, SRes);
        }
        for (int i = 0; i < THREADS; i++)
            threads[i].start();
        for (int i = 0; i < THREADS; i++)
            threads[i].join();
        for (int i =0; i<THREADS;i++)
        {
            S1+=SRes[i][0];
            S2+=SRes[i][1];
        }
        double result = h / 3.0 * (f(x.getFirst()) + f(x.getLast()) + 4 * S1 + 2 * S2);
        var pFinish = System.nanoTime();
        System.out.println("parallel result:");
        System.out.println(result);
        System.out.println("parallel time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void main(String[] args) throws InterruptedException {
        Double a = 0.6, b = 1.4;
        Double h1 = (b - a) / SIZE;
        List<Double> X = new ArrayList<>();
        X.add(a);
        for (int i = 1; i < SIZE+1; i++)
            X.add(X.get(i-1)+h1);
        SimpsonLinear(h1, X);
        SimpsonP(h1, X);
    }
}