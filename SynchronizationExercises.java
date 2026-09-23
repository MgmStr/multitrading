import java.util.concurrent.*;

/*
 * Заготовки к вводной лекции по синхронизации.
 *
 * Компиляция:
 *   javac SynchronizationExercises.java
 *
 * Запуск отдельной задачи, например:
 *   java MutexBankAccountTask
 *
 * В одном файле намеренно находится несколько package-private классов:
 * благодаря этому у каждой задачи есть собственный main, но компилировать
 * студентам нужно только один файл.
 */


class FutureCombinationTask {
    /*
     * ЗАДАЧА 5. Future
     *
     * functions f и g независимы и выполняются долго. Реализуйте parallelSum:
     * запустите обе функции через переданный ExecutorService, получите два
     * Future и верните сумму результатов.
     *
     * Функции должны исполняться параллельно. Не вызывайте f() или g() напрямую
     * из parallelSum и не создавайте внутри метода новые потоки или новый pool.
     */
    static int parallelSum(ExecutorService pool) throws Exception {
        Future<Integer> future1 = pool.submit(new Callable<Integer>() {
            public Integer call() throws Exception{
                return f();
            }
        });
        Future<Integer> future2 = pool.submit(new Callable<Integer>() {
            public Integer call() throws Exception{
                return g();
            }
        });
        return future1.get()+future2.get();
    }

    private static int f() throws InterruptedException {
        Thread.sleep(700);
        return 20;
    }

    private static int g() throws InterruptedException {
        Thread.sleep(700);
        return 22;
    }

    public static void main(String[] args) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        long startedAt = System.nanoTime();

        try {
            int sum = parallelSum(pool);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startedAt);

            if (sum != 42) {
                throw new AssertionError("Expected sum 42, got " + sum);
            }
            if (elapsedMillis >= 1_200) {
                throw new AssertionError(
                        "The result is correct, but f and g probably ran sequentially ("
                                + elapsedMillis + " ms)");
            }

            System.out.println("OK: sum=42, both computations ran in parallel");
        } finally {
            pool.shutdownNow();
        }
    }

    /*
     * Вывод, означающий, что задача, скорее всего, решена правильно:
     * OK: sum=42, both computations ran in parallel
     */
}
//Задачка: Допустим, существует полный порядок ресурсов в программе и ресурсы
//блокируются в соответствие с этим порядком, а освобождаются в обратном порядке.
//Можно ли в таком случае организовать взаимную блокировку? Можно ли организовать
//взаимную блокировку, если отдавать ресурсы в том-же порядке, в каком мы их берём?
// - Взаимная блокировка может возникнуть только, если какой-то поток взял данные не свою очередь,
//а так как по условию у нас есть единый порядок для всей программы, то и никто в очередь влезть не может,
//чтобы забрать данные. В обоих случаях нет

