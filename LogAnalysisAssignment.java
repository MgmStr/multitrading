import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {
        public static LogEntry parse(String line) {
            if (line == null || line.isBlank()) {
                throw new IllegalArgumentException("Empty line");
            }
            String[] parts = line.trim().split("\\s+");
            if (parts.length != 5) {
                throw new IllegalArgumentException("Invalid field count: expected 5, got " + parts.length);
            }
            try {
                String method = parts[0];
                String path = parts[1];
                int status = Integer.parseInt(parts[2]);
                long responseBytes = Long.parseLong(parts[3]);
                long durationMillis = Long.parseLong(parts[4]);

                if (responseBytes < 0 || durationMillis < 0) {
                    throw new IllegalArgumentException("Negative bytes or duration");
                }
                return new LogEntry(method, path, status, responseBytes, durationMillis);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Failed to parse numeric fields: " + e.getMessage());
            }
        }
    }

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis,
            int processedFiles,
            int failedFiles,
            long readLines,
            long validEntries,
            long corruptedEntries,
            long readErrors
    ) {}


    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                return new Iterator<String>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException("Cannot read " + path, exception);
                        }
                    }


                    public void close() {
                        if (closed) return;
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException("Cannot close " + path, exception);
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) throw new NoSuchElementException();
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException("Cannot open " + path, exception);
            }
        }
    }

    private static class MutablePathStats {
        long requests = 0;
        long successful = 0;
        long errors = 0;
        long responseBytes = 0;
        long totalDuration = 0;
        long maxDuration = 0;

        synchronized void add(LogEntry entry) {
            requests++;
            if (entry.status() >= 200 && entry.status() <= 399) successful++;
            else errors++;
            responseBytes += entry.responseBytes();
            totalDuration += entry.durationMillis();
            if (entry.durationMillis() > maxDuration) maxDuration = entry.durationMillis();
        }

        synchronized PathStatistics toImmutable() {
            return new PathStatistics(requests, successful, errors, responseBytes, totalDuration, maxDuration);
        }
    }

    private static class StatisticsAccumulator {

        private final ConcurrentHashMap<String, MutablePathStats> statsMap = new ConcurrentHashMap<>();

        public void add(LogEntry entry) {

            MutablePathStats stats = statsMap.computeIfAbsent(entry.path(), k -> new MutablePathStats());
            stats.add(entry);
        }

        public Map<String, PathStatistics> getSnapshot() {
            Map<String, PathStatistics> result = new HashMap<>();
            for (var e : statsMap.entrySet()) {
                result.put(e.getKey(), e.getValue().toImmutable());
            }
            return result;
        }
    }

    private final AtomicInteger processedFiles = new AtomicInteger();
    private final AtomicInteger failedFiles = new AtomicInteger();
    private final AtomicLong readLines = new AtomicLong();
    private final AtomicLong validEntries = new AtomicLong();
    private final AtomicLong corruptedEntries = new AtomicLong();
    private final AtomicLong readErrors = new AtomicLong();

    private final AtomicInteger activeFiles = new AtomicInteger();
    private final AtomicInteger maxActiveFiles = new AtomicInteger();

    private volatile boolean stopped = false;

    public void stopAnalysis() {
        stopped = true;
    }
    public Report analyzeSequential(Path directory) throws IOException {
        StatisticsAccumulator acc = new StatisticsAccumulator();
        int pFiles = 0, fFiles = 0;
        long rLines = 0, vEntries = 0, cEntries = 0, rErrors = 0;
        long maxDur = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                if (!path.toString().endsWith(".log")) continue;
                try (var it = (AutoCloseable & Iterator<String>) LogFileLines.iterator(path)) {
                    while (it.hasNext()) {
                        String line = it.next();
                        rLines++;
                        try {
                            LogEntry entry = LogEntry.parse(line);
                            vEntries++;
                            acc.add(entry);
                            if (entry.durationMillis() > maxDur) maxDur = entry.durationMillis();
                        } catch (Exception e) {
                            cEntries++;
                        }
                    }
                    pFiles++;
                } catch (Exception e) {
                    fFiles++;
                    rErrors++;
                }
            }
        }

        return buildReport(acc, pFiles, fFiles, rLines, vEntries, cEntries, rErrors, maxDur);
    }

    public Report analyzeParallel(Path directory, int threadCount, int semaphoreLimit) throws IOException, InterruptedException {
        List<Path> files;
        try (var paths = Files.list(directory)) {
            files = paths.filter(p -> p.toString().endsWith(".log")).toList();
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Semaphore semaphore = new Semaphore(semaphoreLimit);
        CountDownLatch latch = new CountDownLatch(files.size());
        StatisticsAccumulator acc = new StatisticsAccumulator();

        processedFiles.set(0); failedFiles.set(0); readLines.set(0);
        validEntries.set(0); corruptedEntries.set(0); readErrors.set(0);
        activeFiles.set(0); maxActiveFiles.set(0);
        stopped = false;

        long globalMaxDuration = 0; // Для простоты в параллельной версии считаем в финальном отчете из PathStatistics

        for (Path file : files) {
            executor.submit(() -> {
                try {

                    semaphore.acquire();

                    int currentActive = activeFiles.incrementAndGet();
                    int max;
                    do {
                        max = maxActiveFiles.get();
                        if (currentActive <= max) break;
                    } while (!maxActiveFiles.compareAndSet(max, currentActive));

                    try {
                        try (var it = (AutoCloseable & Iterator<String>) LogFileLines.iterator(file)) {
                            while (it.hasNext()) {
                                if (stopped) break;

                                String line = it.next();
                                readLines.incrementAndGet();
                                try {
                                    LogEntry entry = LogEntry.parse(line);
                                    validEntries.incrementAndGet();
                                    acc.add(entry);
                                } catch (Exception e) {
                                    corruptedEntries.incrementAndGet();
                                }
                            }
                        }
                        processedFiles.incrementAndGet();
                    } catch (Exception e) {
                        readErrors.incrementAndGet();
                        failedFiles.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    activeFiles.decrementAndGet();
                    semaphore.release();
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        long maxDur = acc.getSnapshot().values().stream()
                .mapToLong(PathStatistics::maxDurationMillis)
                .max().orElse(0);

        return buildReport(acc, processedFiles.get(), failedFiles.get(), readLines.get(),
                validEntries.get(), corruptedEntries.get(), readErrors.get(), maxDur);
    }

    private Report buildReport(StatisticsAccumulator acc, int pFiles, int fFiles, long rLines,
                               long vEntries, long cEntries, long rErrors, long maxDur) {
        long totalReq = 0, successReq = 0, errReq = 0, totalBytes = 0;
        for (PathStatistics ps : acc.getSnapshot().values()) {
            totalReq += ps.requests();
            successReq += ps.successful();
            errReq += ps.errors();
            totalBytes += ps.responseBytes();
        }
        return new Report(totalReq, successReq, errReq, totalBytes, maxDur, pFiles, fFiles, rLines, vEntries, cEntries, rErrors);
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {"GET ", "POST ", "PUT ", "DELETE "};
        private static final String[] PATHS = {"/ ", "/index.html ", "/login ", "/api/users ", "/api/orders ", "/api/products ", "/search ", "/health "};
        private static final int[] STATUSES = {200, 200, 200, 201, 204, 301, 400, 401, 404, 500};

        private GenerateLogs() {}

        public static void generate(Path outputDirectory, int fileCount, int linesPerFile) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) throw new IllegalArgumentException("Counts must be non-negative");
            Files.createDirectories(outputDirectory);
            Random random = new Random(42);
            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve("access-%03d.log".formatted(fileId));
                try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method = METHODS[random.nextInt(METHODS.length)];
                        String path = PATHS[random.nextInt(PATHS.length)];
                        int status = STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes = 100L + random.nextInt(100_000);
                        long durationMillis = 1L + random.nextInt(500);
                        writer.write("%s %s %d %d %d%n".formatted(method, path, status, responseBytes, durationMillis));
                    }
                }
            }
        }
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);
        int files = 0; int lines = 0;
        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) throw new AssertionError("Unexpected line: " + line);
                    Integer.parseInt(fields[2]); Long.parseLong(fields[3]); Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }
        if (files != 3 || lines != 21) throw new AssertionError("Expected 3 files and 21 lines");
        System.out.println("OK: generated " + files + " files and read " + lines + " lines");
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int fileCount = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int linesPerFile = args.length > 3 ? Integer.parseInt(args[3]) : 1_000;
            GenerateLogs.generate(directory, fileCount, linesPerFile);
            System.out.println("Generated " + fileCount + " files in " + directory);
            return;
        }
        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }
        if (args.length > 0 && args[0].equals("analyze")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int semLimit = args.length > 3 ? Integer.parseInt(args[3]) : 4;

            LogAnalysisAssignment analyzer = new LogAnalysisAssignment();

            System.out.println("Running sequential analysis...");
            long startSeq = System.currentTimeMillis();
            Report seqReport = analyzer.analyzeSequential(directory);
            long timeSeq = System.currentTimeMillis() - startSeq;

            System.out.println("Running parallel analysis (threads=" + threads + ", sem=" + semLimit + ")...");
            long startPar = System.currentTimeMillis();
            Report parReport = analyzer.analyzeParallel(directory, threads, semLimit);
            long timePar = System.currentTimeMillis() - startPar;

            System.out.println("\n--- Sequential Report ---");
            printReport(seqReport, timeSeq);

            System.out.println("\n--- Parallel Report ---");
            printReport(parReport, timePar);

            // Проверка инварианта: результаты должны совпадать
            if (seqReport.totalRequests() == parReport.totalRequests() &&
                    seqReport.responseBytes() == parReport.responseBytes()) {
                System.out.println("\n[OK] Invariant passed: Sequential and Parallel reports match.");
            } else {
                System.out.println("\n[FAIL] Reports do not match!");
            }
            return;
        }

        System.out.println("Usage:");
        System.out.println("  java LogAnalysisAssignment generate <directory> <files> <lines>");
        System.out.println("  java LogAnalysisAssignment check");
        System.out.println("  java LogAnalysisAssignment analyze <directory> <threads> <semaphore_limit>");
    }

    private static void printReport(Report r, long timeMs) {
        System.out.printf("Total Requests: %d (Success: %d, Errors: %d)%n", r.totalRequests(), r.successfulRequests(), r.errorRequests());
        System.out.printf("Response Bytes: %d, Max Duration: %d ms%n", r.responseBytes(), r.maxDurationMillis());
        System.out.printf("Files: Processed=%d, Failed=%d%n", r.processedFiles(), r.failedFiles());
        System.out.printf("Lines: Read=%d, Valid=%d, Corrupted=%d, ReadErrors=%d%n",
                r.readLines(), r.validEntries(), r.corruptedEntries(), r.readErrors());
        System.out.printf("Time: %d ms%n", timeMs);
    }
}