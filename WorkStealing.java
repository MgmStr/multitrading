import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable{
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor{}
    static class ThreadPerTaskExecutor implements ShutdownableExecutor{
        List<Thread> threads = new ArrayList<>();
        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }
        public void shutdown(){
            threads.forEach(t -> {try {t.join();} catch (InterruptedException e) {throw new RuntimeException(e);}});
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {};

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads){
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x->x<threads, x->x+1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x-> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id){
            return new Thread(() ->
                {
                    while (true) {
                        Runnable task;
                        try {
                            task = tasks.get(id).takeFirst();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == EXIT_TASK)
                            return;
                        task.run();
                    }
                }
            );
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }
            return null;
        }
    }

    // Это вещи, которые нам нужны
    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000; //ms = 10 s

    // Это всякое вспомогательное побочное
    static final int MEAN_TASK_TIME =  (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER); //ms
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2+1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    public static double f(double x){
        return x*x*5+53*x+x*Math.cos(x)*4;
    }

    static void BlackHole(long difficulty)
    {
        Double a = 0.6, b = 1.4;
        List<Double> X = new ArrayList<>();
        X.add(a);
        Double h1 = (b - a) / difficulty;
        for (int i = 1; i < difficulty+1; i++)
            X.add(X.get(i-1)+h1);
        double S1 = 0, S2 = 0;
        for (int i = 1; i < difficulty; ++i)
        {
            if (i % 2 == 0)
                S2 += f(X.get(i));
            else
                S1 += f(X.get(i));
        }
        double result = h1 / 3.0 * (f(X.getFirst()) + f(X.getLast()) + 4 * S1 + 2 * S2);
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService executorService;

        FixedThreadPoolExecutor(int threads) {
            this.executorService = Executors.newFixedThreadPool(threads);
        }
        @Override
        public void execute(Runnable command) {
            executorService.execute(command);
        }
        @Override
        public void shutdown() {
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static Runnable createTask(int duration){
        return () -> {
            BlackHole(duration);
        };
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        // var random = java.util.concurrent.ThreadLocalRandom.current();
        var durations = new int[TASK_NUMBER];

        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution){
        var durations = createTaskDurations(distribution);
        return new Vector<>(
                IntStream
                    .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                    .mapToObj(x -> createTask(durations[x]))
                    .toList()
        );
    }

    public record Pair<A,B>(A first, B second){};

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution){
        var tasks = createTasks(distribution);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submitionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submitionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);

        // В теории, на винде вот эта строчка может навернуть систему, будьте аккуратны!
        var threadPerTaskExecutorResult = measureExecutor(new ThreadPerTaskExecutor(), TaskDistribution.UNIFORM);

        System.out.println("Thread per task executor submtition time: " + threadPerTaskExecutorResult.first/1_000_000d);
        System.out.println("Thread per task execution time: " + threadPerTaskExecutorResult.second/1_000_000d);

        for (var distribution : TaskDistribution.values()) {
            System.out.println("Task distribution: " + distribution);

            var roundRobinExecutorResult = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution);

            System.out.println("Round Robin Executor submition time: " + roundRobinExecutorResult.first/1_000_000d);
            System.out.println("Round Robin Executor execution time: " + roundRobinExecutorResult.second/1_000_000d);

            var workStealingExecutorResult = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution);

            System.out.println("Work Stealing Executor submition time: " + workStealingExecutorResult.first/1_000_000d);
            System.out.println("Work Stealing Executor execution time: " + workStealingExecutorResult.second/1_000_000d);

            var fixedThreadPoolResult = measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), distribution);
            System.out.println("FixedThreadPool Executor submition time: " + fixedThreadPoolResult.first / 1_000_000d);
            System.out.println("FixedThreadPool Executor execution time: " + fixedThreadPoolResult.second / 1_000_000d);
        }
        }
    }

//результаты тестов после введения BlackHole и FixedThreadpool:

//Thread per task executor submtition time: 9039.155348
//Thread per task execution time: 9051.365007
//Task distribution: UNIFORM
//Round Robin Executor submition time: 148.211099
//Round Robin Executor execution time: 149.025514
//Work Stealing Executor submition time: 52.727162
//Work Stealing Executor execution time: 55.329569
//FixedThreadPool Executor submition time: 25.778599
//FixedThreadPool Executor execution time: 56.799445
//Task distribution: PERIODIC
//Round Robin Executor submition time: 143.663406
//Round Robin Executor execution time: 148.04489
//Work Stealing Executor submition time: 59.151771
//Work Stealing Executor execution time: 59.34913
//FixedThreadPool Executor submition time: 19.272648
//FixedThreadPool Executor execution time: 28.603634
//Task distribution: PARETO
//Round Robin Executor submition time: 99.382178
//Round Robin Executor execution time: 99.784185
//Work Stealing Executor submition time: 84.973676
//Work Stealing Executor execution time: 85.540676
//FixedThreadPool Executor submition time: 8.649755
//FixedThreadPool Executor execution time: 18.997265

//в первую очередь стоит отметить, что после введения функции с реальной вычислительной задачей заместо sleep,
//workstealing реально начал работать ускоряя программу. При сравнении дополнительно введённого исполнителя FixedThreadPoolExecutor,
//заметно его быстродействие относительно остальных. Скорость выполнения повышается за счёт того, что fixedthreadpool может оптимально
// перераспределять потоки, в зависимости от очереди задач

