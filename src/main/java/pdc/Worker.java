package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 *
 * Connects to Master, receives matrix computation tasks via RPC,
 * executes them in parallel, and returns results.
 */
public class Worker {

    // Configuration constants
    private static final int TASK_THREAD_POOL_SIZE = 4;
    private static final String DEFAULT_WORKER_ID_PREFIX = "worker-";
    private static final String DEFAULT_STUDENT_ID = "unknown";

    // Worker state
    private String workerId;
    private String studentId;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private volatile boolean running = true;
    private final ExecutorService taskExecutor;

    public Worker() {
        this.taskExecutor = Executors.newFixedThreadPool(TASK_THREAD_POOL_SIZE);
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     *
     * @param masterHost hostname of the master server
     * @param port port number of the master server
     */
    public void joinCluster(String masterHost, int port) {
        try {
            initializeIdentity();
            connectToMaster(masterHost, port);
            performRegistration();
            execute();
        } catch (IOException e) {
            running = false;
            cleanup();
            // Handle connection failures gracefully - don't throw
        }
    }

    /**
     * Initializes worker and student IDs from environment variables.
     */
    private void initializeIdentity() {
        workerId = System.getenv("WORKER_ID");
        if (workerId == null || workerId.isEmpty()) {
            workerId = DEFAULT_WORKER_ID_PREFIX + System.currentTimeMillis();
        }

        studentId = System.getenv("STUDENT_ID");
        if (studentId == null || studentId.isEmpty()) {
            studentId = DEFAULT_STUDENT_ID;
        }
    }

    /**
     * Establishes socket connection to the master.
     */
    private void connectToMaster(String masterHost, int port) throws IOException {
        socket = new Socket(masterHost, port);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);

        input = new DataInputStream(socket.getInputStream());
        output = new DataOutputStream(socket.getOutputStream());
    }

    /**
     * Performs the registration handshake with the master.
     */
    private void performRegistration() throws IOException {
        Message registerMsg = Message.createRegister(studentId, workerId);
        synchronized (output) {
            registerMsg.writeToStream(output);
        }

        Message ack = Message.readFromStream(input);
        if (!"REGISTER_ACK".equals(ack.messageType)) {
            throw new IOException("Expected REGISTER_ACK, got: " + ack.messageType);
        }
    }

    /**
     * Executes a received task block.
     * Starts a listener thread that processes incoming messages.
     */
    public void execute() {
        Thread listener = new Thread(this::messageLoop, "Worker-" + workerId + "-Listener");
        listener.setDaemon(true);
        listener.start();
    }

    /**
     * Main message processing loop.
     */
    private void messageLoop() {
        while (running) {
            try {
                Message msg = Message.readFromStream(input);
                handleMessage(msg);
            } catch (IOException e) {
                if (running) {
                    running = false;
                    cleanup();
                }
                break;
            }
        }
    }

    /**
     * Routes incoming messages to appropriate handlers.
     */
    private void handleMessage(Message msg) {
        switch (msg.messageType) {
            case "HEARTBEAT":
                sendHeartbeatAck();
                break;
            case "RPC_REQUEST":
                taskExecutor.submit(() -> processTask(msg));
                break;
            default:
                // Ignore unknown message types
                break;
        }
    }

    /**
     * Sends a heartbeat acknowledgment to the master.
     */
    private void sendHeartbeatAck() {
        try {
            Message ack = Message.createHeartbeatAck(studentId, workerId);
            synchronized (output) {
                ack.writeToStream(output);
            }
        } catch (IOException e) {
            running = false;
        }
    }

    /**
     * Processes an RPC request for matrix computation.
     * Payload format: taskId;taskType;matrixData
     * Matrix data format: rows;cols;val1,val2,...|rows;cols;val1,val2,...
     */
    private void processTask(Message request) {
        String taskId = null;
        try {
            String payloadStr = new String(request.payload, StandardCharsets.UTF_8);

            // Parse taskId
            int firstSemi = payloadStr.indexOf(';');
            taskId = payloadStr.substring(0, firstSemi);

            // Parse taskType
            int secondSemi = payloadStr.indexOf(';', firstSemi + 1);

            // Parse matrix data
            String matrixData = payloadStr.substring(secondSemi + 1);
            String[] matrixParts = matrixData.split("\\|");

            int[][] matrixA = decodeMatrix(matrixParts[0]);
            int[][] matrixB = decodeMatrix(matrixParts[1]);

            // Perform computation
            int[][] result = multiplyMatrices(matrixA, matrixB);

            // Send response
            byte[] resultData = encodeMatrix(result).getBytes(StandardCharsets.UTF_8);
            Message response = Message.createRpcResponse(studentId, taskId, resultData);
            synchronized (output) {
                response.writeToStream(output);
            }

        } catch (Exception e) {
            sendErrorResponse(taskId, e);
        }
    }

    /**
     * Sends an error response for a failed task.
     */
    private void sendErrorResponse(String taskId, Exception error) {
        if (taskId == null) {
            return;
        }

        try {
            String errorMsg = (error.getMessage() != null) ? error.getMessage() : "Unknown error";
            Message errorResponse = Message.createTaskError(studentId, taskId, errorMsg);
            synchronized (output) {
                errorResponse.writeToStream(output);
            }
        } catch (IOException ioe) {
            running = false;
        }
    }

    /**
     * Decodes a matrix from string format: rows;cols;val1,val2,val3,...
     *
     * @param encoded the encoded matrix string
     * @return decoded 2D integer array
     */
    private int[][] decodeMatrix(String encoded) {
        String[] parts = encoded.split(";");
        int rows = Integer.parseInt(parts[0]);
        int cols = Integer.parseInt(parts[1]);

        int[][] matrix = new int[rows][cols];

        if (parts.length > 2 && !parts[2].isEmpty()) {
            String[] values = parts[2].split(",");
            int expectedSize = rows * cols;
            if (values.length != expectedSize) {
                throw new IllegalArgumentException(
                    "Matrix data size mismatch: expected " + expectedSize + ", got " + values.length);
            }

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
     * Encodes a matrix to string format: rows;cols;val1,val2,val3,...
     *
     * @param matrix the matrix to encode
     * @return encoded string representation
     */
    private String encodeMatrix(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;

        StringBuilder sb = new StringBuilder();
        sb.append(rows).append(";").append(cols).append(";");

        boolean first = true;
        for (int[] row : matrix) {
            for (int val : row) {
                if (!first) {
                    sb.append(",");
                }
                sb.append(val);
                first = false;
            }
        }

        return sb.toString();
    }

    /**
     * Performs matrix multiplication: C = A × B
     *
     * @param a first matrix
     * @param b second matrix
     * @return result matrix
     */
    private int[][] multiplyMatrices(int[][] a, int[][] b) {
        int rowsA = a.length;
        int colsA = a[0].length;
        int colsB = b[0].length;

        int[][] result = new int[rowsA][colsB];

        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += a[i][k] * b[k][j];
                }
                result[i][j] = sum;
            }
        }

        return result;
    }

    /**
     * Cleans up resources on shutdown.
     */
    private void cleanup() {
        running = false;

        shutdownExecutor();
        closeSocket();
    }

    /**
     * Shuts down the task executor.
     */
    private void shutdownExecutor() {
        if (taskExecutor != null) {
            taskExecutor.shutdownNow();
        }
    }

    /**
     * Closes the socket connection.
     */
    private void closeSocket() {
        if (socket == null || socket.isClosed()) {
            return;
        }

        try {
            socket.close();
        } catch (IOException e) {
            // Expected during shutdown, socket may already be closed
        }
    }

    /**
     * Stops the worker gracefully.
     */
    public void shutdown() {
        cleanup();
    }

    /**
     * Checks if the worker is still running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
}
