package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 *
 * Handles worker registration, task distribution, heartbeat monitoring,
 * failure detection, and result aggregation.
 */
public class Master {

    // Configuration constants
    private static final int HEARTBEAT_INTERVAL_MS = 3000;
    private static final int HEARTBEAT_TIMEOUT_MS = 10000;
    private static final int TASK_TIMEOUT_MS = 30000;
    private static final int COORDINATION_TIMEOUT_SECONDS = 60;
    private static final int MAX_TASK_RETRIES = 3;
    private static final int WORKER_WAIT_ATTEMPTS = 50;
    private static final int WORKER_WAIT_INTERVAL_MS = 100;
    private static final String DEFAULT_STUDENT_ID = "unknown";

    // Thread pool for system operations
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();

    // Server state
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private String studentId;

    // Worker tracking (thread-safe)
    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<String> workerIds = new CopyOnWriteArrayList<>();

    // Task management
    private final ConcurrentHashMap<String, TaskInfo> activeTasks = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<TaskInfo> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, byte[]> taskResults = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CountDownLatch> taskLatches = new ConcurrentHashMap<>();

    // Task ID generator
    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);

    /**
     * Represents a connection to a worker node.
     */
    private static class WorkerConnection {
        final String workerId;
        final Socket socket;
        final DataInputStream input;
        final DataOutputStream output;
        final AtomicLong lastHeartbeat = new AtomicLong();
        final AtomicBoolean alive = new AtomicBoolean(true);
        final AtomicInteger activeTaskCount = new AtomicInteger(0);

        WorkerConnection(String workerId, Socket socket,
                         DataInputStream input, DataOutputStream output) {
            this.workerId = workerId;
            this.socket = socket;
            this.input = input;
            this.output = output;
            this.lastHeartbeat.set(System.currentTimeMillis());
        }
    }

    /**
     * Represents information about a distributed task.
     */
    private static class TaskInfo {
        final String taskId;
        final String taskType;
        final byte[] payload;
        final int startRow;
        final int rowCount;
        final String coordinationId;
        final AtomicInteger retryCount = new AtomicInteger(0);

        volatile String assignedWorker;
        volatile long assignedTime;

        TaskInfo(String taskId, String taskType, byte[] payload,
                 int startRow, int rowCount, String coordinationId) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.payload = payload;
            this.startRow = startRow;
            this.rowCount = rowCount;
            this.coordinationId = coordinationId;
        }
    }

    /**
     * Entry point for a distributed computation.
     *
     * @param operation   A string descriptor of the matrix operation
     * @param data        The raw matrix data to be processed
     * @param workerCount Number of workers to distribute to
     * @return The computed result matrix, or null if computation fails
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        if (!waitForWorkers()) {
            return null;
        }

        int activeWorkers = Math.min(workerCount, workers.size());
        if (activeWorkers == 0) {
            return null;
        }

        List<TaskInfo> tasks = createTasks(operation, data, activeWorkers);
        CountDownLatch completionLatch = new CountDownLatch(tasks.size());
        registerTaskLatches(tasks, completionLatch);
        submitTasks(tasks);

        boolean completed = awaitCompletion(completionLatch, tasks);
        if (!completed) {
            cancelRemainingTasks(tasks);
        }

        return assembleResults(tasks, data[0].length);
    }

    /**
     * Waits for at least one worker to connect.
     */
    private boolean waitForWorkers() {
        int attempts = 0;
        while (workers.isEmpty() && attempts < WORKER_WAIT_ATTEMPTS) {
            try {
                Thread.sleep(WORKER_WAIT_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            attempts++;
        }
        return !workers.isEmpty();
    }

    /**
     * Creates tasks for distributing matrix computation across workers.
     */
    private List<TaskInfo> createTasks(String operation, int[][] data, int activeWorkers) {
        int rows = data.length;
        int cols = data[0].length;
        int rowsPerWorker = Math.max(1, (rows + activeWorkers - 1) / activeWorkers);

        String coordId = "coord-" + System.currentTimeMillis();
        int[][] matrixB = MatrixGenerator.generateIdentityMatrix(cols);

        List<TaskInfo> tasks = new ArrayList<>();

        for (int startRow = 0; startRow < rows; startRow += rowsPerWorker) {
            int endRow = Math.min(startRow + rowsPerWorker, rows);
            int rowCount = endRow - startRow;

            String taskId = "task-" + taskIdGenerator.incrementAndGet();
            byte[] payload = encodeRowBlockAndMatrix(data, startRow, endRow, matrixB);

            tasks.add(new TaskInfo(taskId, operation, payload, startRow, rowCount, coordId));
        }

        return tasks;
    }

    /**
     * Registers completion latches for all tasks.
     */
    private void registerTaskLatches(List<TaskInfo> tasks, CountDownLatch latch) {
        for (TaskInfo task : tasks) {
            taskLatches.put(task.taskId, latch);
        }
    }

    /**
     * Submits all tasks to the task queue.
     */
    private void submitTasks(List<TaskInfo> tasks) {
        for (TaskInfo task : tasks) {
            taskQueue.offer(task);
        }
    }

    /**
     * Awaits completion of all tasks with timeout.
     */
    private boolean awaitCompletion(CountDownLatch latch, List<TaskInfo> tasks) {
        try {
            return latch.await(COORDINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Cancels remaining tasks on timeout.
     */
    private void cancelRemainingTasks(List<TaskInfo> tasks) {
        for (TaskInfo task : tasks) {
            activeTasks.remove(task.taskId);
            taskLatches.remove(task.taskId);
        }
    }

    /**
     * Encodes a row block of matrix A and full matrix B for transmission.
     * Format: rows;cols;val1,val2,...|rows;cols;val1,val2,...
     */
    private byte[] encodeRowBlockAndMatrix(int[][] matrixA, int startRow, int endRow, int[][] matrixB) {
        StringBuilder sb = new StringBuilder();

        // Encode row block of A
        appendMatrixBlock(sb, matrixA, startRow, endRow);
        sb.append("|");

        // Encode full matrix B
        appendMatrixBlock(sb, matrixB, 0, matrixB.length);

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Appends a matrix block to the StringBuilder.
     */
    private void appendMatrixBlock(StringBuilder sb, int[][] matrix, int startRow, int endRow) {
        int rowCount = endRow - startRow;
        int cols = matrix[0].length;

        sb.append(rowCount).append(";").append(cols).append(";");

        boolean first = true;
        for (int i = startRow; i < endRow; i++) {
            for (int j = 0; j < cols; j++) {
                if (!first) {
                    sb.append(",");
                }
                sb.append(matrix[i][j]);
                first = false;
            }
        }
    }

    /**
     * Assembles results from completed tasks into final matrix.
     */
    private int[][] assembleResults(List<TaskInfo> tasks, int resultCols) {
        int totalRows = tasks.stream().mapToInt(t -> t.rowCount).sum();
        int[][] result = new int[totalRows][resultCols];

        for (TaskInfo task : tasks) {
            byte[] resultData = taskResults.remove(task.taskId);
            taskLatches.remove(task.taskId);

            if (resultData != null) {
                int[][] taskResult = decodeMatrix(new String(resultData, StandardCharsets.UTF_8));
                copyTaskResult(result, taskResult, task.startRow, resultCols);
            }
        }

        return result;
    }

    /**
     * Copies a task's result into the appropriate position in the final result.
     */
    private void copyTaskResult(int[][] result, int[][] taskResult, int startRow, int cols) {
        for (int i = 0; i < taskResult.length; i++) {
            System.arraycopy(taskResult[i], 0, result[startRow + i], 0, cols);
        }
    }

    /**
     * Decodes a matrix from string format: rows;cols;val1,val2,...
     */
    private int[][] decodeMatrix(String encoded) {
        String[] parts = encoded.split(";");
        int rows = Integer.parseInt(parts[0]);
        int cols = Integer.parseInt(parts[1]);

        int[][] matrix = new int[rows][cols];

        if (parts.length > 2 && !parts[2].isEmpty()) {
            String[] values = parts[2].split(",");
            int idx = 0;
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    matrix[i][j] = Integer.parseInt(values[idx++]);
                }
            }
        }

        return matrix;
    }

    /**
     * Starts the communication listener on the specified port.
     *
     * @param port the port to listen on
     * @throws IOException if socket creation fails
     */
    public void listen(int port) throws IOException {
        studentId = getStudentId();
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);

        startBackgroundThreads();
    }

    /**
     * Gets the student ID from environment variables.
     */
    private String getStudentId() {
        String id = System.getenv("STUDENT_ID");
        return (id != null && !id.isEmpty()) ? id : DEFAULT_STUDENT_ID;
    }

    /**
     * Starts all background processing threads.
     */
    private void startBackgroundThreads() {
        systemThreads.submit(this::acceptLoop);
        systemThreads.submit(this::heartbeatLoop);
        systemThreads.submit(this::taskDispatcherLoop);
        systemThreads.submit(this::taskTimeoutLoop);
    }

    /**
     * Accept loop - handles incoming worker connections.
     */
    private void acceptLoop() {
        Thread.currentThread().setName("Master-AcceptLoop");

        while (running) {
            try {
                Socket client = serverSocket.accept();
                systemThreads.submit(() -> handleConnection(client));
            } catch (IOException e) {
                if (running) {
                    // Connection accept failed, continue listening
                }
            }
        }
    }

    /**
     * Handles a new worker connection.
     */
    private void handleConnection(Socket socket) {
        Thread.currentThread().setName("Master-ConnectionHandler");

        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            Message reg = Message.readFromStream(in);

            if ("REGISTER".equals(reg.messageType)) {
                registerWorker(socket, in, out, reg);
            }
        } catch (IOException e) {
            closeSocketQuietly(socket);
        }
    }

    /**
     * Registers a new worker and starts its response listener.
     */
    private void registerWorker(Socket socket, DataInputStream in,
                                DataOutputStream out, Message reg) throws IOException {
        String workerId = new String(reg.payload, StandardCharsets.UTF_8);

        WorkerConnection conn = new WorkerConnection(workerId, socket, in, out);
        workers.put(workerId, conn);
        workerIds.add(workerId);

        Message ack = Message.createRegisterAck(studentId);
        synchronized (out) {
            ack.writeToStream(out);
        }

        systemThreads.submit(() -> responseListener(conn));
    }

    /**
     * Listens for responses from a worker.
     */
    private void responseListener(WorkerConnection conn) {
        Thread.currentThread().setName("Master-ResponseListener-" + conn.workerId);

        while (running && conn.alive.get()) {
            try {
                Message msg = Message.readFromStream(conn.input);
                handleWorkerMessage(conn, msg);
            } catch (IOException e) {
                if (running && conn.alive.get()) {
                    handleWorkerFailure(conn);
                }
                break;
            }
        }
    }

    /**
     * Routes worker messages to appropriate handlers.
     */
    private void handleWorkerMessage(WorkerConnection conn, Message msg) {
        switch (msg.messageType) {
            case "HEARTBEAT_ACK":
                conn.lastHeartbeat.set(System.currentTimeMillis());
                break;
            case "RPC_RESPONSE":
                handleRpcResponse(conn, msg);
                break;
            case "TASK_ERROR":
                handleTaskError(conn, msg);
                break;
            default:
                // Ignore unknown message types
                break;
        }
    }

    /**
     * Handles an RPC response from a worker.
     */
    private void handleRpcResponse(WorkerConnection conn, Message msg) {
        String payloadStr = new String(msg.payload, StandardCharsets.UTF_8);

        int semicolon = payloadStr.indexOf(';');
        if (semicolon < 0) {
            return;
        }

        String taskId = payloadStr.substring(0, semicolon);
        byte[] resultData = payloadStr.substring(semicolon + 1).getBytes(StandardCharsets.UTF_8);

        taskResults.put(taskId, resultData);

        TaskInfo task = activeTasks.remove(taskId);
        if (task != null) {
            conn.activeTaskCount.decrementAndGet();
        }

        CountDownLatch latch = taskLatches.get(taskId);
        if (latch != null) {
            latch.countDown();
        }
    }

    /**
     * Handles a task error from a worker.
     */
    private void handleTaskError(WorkerConnection conn, Message msg) {
        String payloadStr = new String(msg.payload, StandardCharsets.UTF_8);

        int semicolon = payloadStr.indexOf(';');
        if (semicolon < 0) {
            return;
        }

        String taskId = payloadStr.substring(0, semicolon);

        TaskInfo task = activeTasks.remove(taskId);
        if (task != null) {
            conn.activeTaskCount.decrementAndGet();
            reassignTask(task);
        }
    }

    /**
     * Heartbeat loop - sends heartbeats and detects dead workers.
     */
    private void heartbeatLoop() {
        Thread.currentThread().setName("Master-HeartbeatLoop");

        while (running) {
            sleepInterruptibly(HEARTBEAT_INTERVAL_MS);
            if (!running) break;

            long now = System.currentTimeMillis();

            for (WorkerConnection w : workers.values()) {
                if (!w.alive.get()) continue;

                if (now - w.lastHeartbeat.get() > HEARTBEAT_TIMEOUT_MS) {
                    handleWorkerFailure(w);
                    continue;
                }

                sendHeartbeat(w);
            }
        }
    }

    /**
     * Sends a heartbeat to a worker.
     */
    private void sendHeartbeat(WorkerConnection worker) {
        try {
            Message heartbeat = Message.createHeartbeat(studentId);
            synchronized (worker.output) {
                heartbeat.writeToStream(worker.output);
            }
        } catch (IOException e) {
            handleWorkerFailure(worker);
        }
    }

    /**
     * Task dispatcher loop - assigns queued tasks to available workers.
     */
    private void taskDispatcherLoop() {
        Thread.currentThread().setName("Master-TaskDispatcher");

        while (running) {
            try {
                TaskInfo task = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                dispatchTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Dispatches a task to an available worker.
     */
    private void dispatchTask(TaskInfo task) throws InterruptedException {
        WorkerConnection worker = findAvailableWorker();

        if (worker == null) {
            taskQueue.offer(task);
            Thread.sleep(50);
            return;
        }

        task.assignedWorker = worker.workerId;
        task.assignedTime = System.currentTimeMillis();
        activeTasks.put(task.taskId, task);
        worker.activeTaskCount.incrementAndGet();

        try {
            Message request = Message.createRpcRequest(studentId, task.taskId, task.taskType, task.payload);
            synchronized (worker.output) {
                request.writeToStream(worker.output);
            }
        } catch (IOException e) {
            activeTasks.remove(task.taskId);
            worker.activeTaskCount.decrementAndGet();
            handleWorkerFailure(worker);
            reassignTask(task);
        }
    }

    /**
     * Finds an available worker with the lowest task count.
     */
    private WorkerConnection findAvailableWorker() {
        WorkerConnection best = null;
        int lowestCount = Integer.MAX_VALUE;

        for (WorkerConnection w : workers.values()) {
            if (w.alive.get()) {
                int count = w.activeTaskCount.get();
                if (count < lowestCount) {
                    lowestCount = count;
                    best = w;
                }
            }
        }

        return best;
    }

    /**
     * Task timeout loop - detects stalled tasks and reassigns them.
     */
    private void taskTimeoutLoop() {
        Thread.currentThread().setName("Master-TaskTimeoutLoop");

        while (running) {
            sleepInterruptibly(1000);
            if (!running) break;

            long now = System.currentTimeMillis();

            for (TaskInfo task : activeTasks.values()) {
                if (now - task.assignedTime > TASK_TIMEOUT_MS) {
                    handleTaskTimeout(task);
                }
            }
        }
    }

    /**
     * Handles a task that has timed out.
     */
    private void handleTaskTimeout(TaskInfo task) {
        TaskInfo removedTask = activeTasks.remove(task.taskId);
        if (removedTask == null) {
            return;
        }

        WorkerConnection worker = workers.get(removedTask.assignedWorker);
        if (worker != null) {
            worker.activeTaskCount.decrementAndGet();
        }

        reassignTask(removedTask);
    }

    /**
     * Handles worker failure - removes worker and reassigns its tasks.
     */
    private void handleWorkerFailure(WorkerConnection worker) {
        if (!worker.alive.compareAndSet(true, false)) {
            return; // Already handled
        }

        workers.remove(worker.workerId);
        workerIds.remove(worker.workerId);

        closeSocketQuietly(worker.socket);

        reassignWorkerTasks(worker);
    }

    /**
     * Reassigns all active tasks from a failed worker.
     */
    private void reassignWorkerTasks(WorkerConnection worker) {
        for (TaskInfo task : activeTasks.values()) {
            if (worker.workerId.equals(task.assignedWorker)) {
                TaskInfo removedTask = activeTasks.remove(task.taskId);
                if (removedTask != null) {
                    reassignTask(removedTask);
                }
            }
        }
    }

    /**
     * Reassigns a task to the queue for retry.
     */
    private void reassignTask(TaskInfo task) {
        if (task.retryCount.incrementAndGet() <= MAX_TASK_RETRIES) {
            task.assignedWorker = null;
            taskQueue.offer(task);
        } else {
            // Max retries exceeded, signal completion with no result
            CountDownLatch latch = taskLatches.get(task.taskId);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    /**
     * System Health Check - detects dead workers and stalled tasks.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();

        // Dead worker detection
        for (WorkerConnection w : workers.values()) {
            if (w.alive.get() && now - w.lastHeartbeat.get() > HEARTBEAT_TIMEOUT_MS) {
                handleWorkerFailure(w);
            }
        }

        // Stalled task detection
        for (TaskInfo task : activeTasks.values()) {
            if (now - task.assignedTime > TASK_TIMEOUT_MS) {
                handleTaskTimeout(task);
            }
        }
    }

    /**
     * Shuts down the master gracefully.
     */
    public void shutdown() {
        running = false;

        shutdownThreadPool();
        closeAllWorkerConnections();
        closeServerSocket();
    }

    /**
     * Shuts down the thread pool.
     */
    private void shutdownThreadPool() {
        systemThreads.shutdownNow();
    }

    /**
     * Closes all worker connections.
     */
    private void closeAllWorkerConnections() {
        for (WorkerConnection w : workers.values()) {
            closeSocketQuietly(w.socket);
        }
    }

    /**
     * Closes the server socket.
     */
    private void closeServerSocket() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Expected during shutdown
            }
        }
    }

    /**
     * Closes a socket without throwing exceptions.
     */
    private void closeSocketQuietly(Socket socket) {
        if (socket == null || socket.isClosed()) {
            return;
        }

        try {
            socket.close();
        } catch (IOException e) {
            // Expected during cleanup
        }
    }

    /**
     * Sleeps for the specified duration, handling interrupts.
     */
    private void sleepInterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the number of connected workers.
     */
    public int getWorkerCount() {
        return (int) workers.values().stream().filter(w -> w.alive.get()).count();
    }

    /**
     * Checks if the master is running.
     */
    public boolean isRunning() {
        return running;
    }
}
