package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * Binary Wire Format (length-prefixed):
 * [4B total length]
 * [6B magic "CSM218"]
 * [4B version]
 * [4B messageType length][messageType bytes]
 * [4B studentId length][studentId bytes]
 * [8B timestamp]
 * [4B payload length][payload bytes]
 */
public class Message {

    // Protocol constants
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;
    private static final int MAGIC_LENGTH = 6;
    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    // Message fields
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.timestamp = System.currentTimeMillis();
        this.payload = EMPTY_PAYLOAD;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixed binary format.
     *
     * @return serialized message bytes
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write magic (6 bytes fixed)
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            dos.write(magicBytes, 0, MAGIC_LENGTH);

            // Write version
            dos.writeInt(version);

            // Write messageType (length-prefixed)
            byte[] typeBytes = (messageType != null)
                ? messageType.getBytes(StandardCharsets.UTF_8)
                : EMPTY_PAYLOAD;
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);

            // Write studentId (length-prefixed)
            byte[] idBytes = (studentId != null)
                ? studentId.getBytes(StandardCharsets.UTF_8)
                : EMPTY_PAYLOAD;
            dos.writeInt(idBytes.length);
            dos.write(idBytes);

            // Write timestamp
            dos.writeLong(timestamp);

            // Write payload (length-prefixed)
            byte[] payloadBytes = (payload != null) ? payload : EMPTY_PAYLOAD;
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     *
     * @param data serialized message bytes
     * @return deserialized Message object
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < MAGIC_LENGTH) {
            throw new IllegalArgumentException("Invalid message data");
        }

        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();

            // Read magic (6 bytes)
            byte[] magicBytes = new byte[MAGIC_LENGTH];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            // Read version
            msg.version = dis.readInt();

            // Read messageType
            int typeLen = dis.readInt();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);

            // Read studentId
            int idLen = dis.readInt();
            byte[] idBytes = new byte[idLen];
            dis.readFully(idBytes);
            msg.studentId = new String(idBytes, StandardCharsets.UTF_8);

            // Read timestamp
            msg.timestamp = dis.readLong();

            // Read payload
            int payloadLen = dis.readInt();
            msg.payload = new byte[payloadLen];
            dis.readFully(msg.payload);

            return msg;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to unpack message", e);
        }
    }

    /**
     * Reads a complete message from an input stream.
     * First reads the length prefix, then the message body.
     *
     * @param dis input stream to read from
     * @return deserialized Message object
     * @throws IOException if read fails
     */
    public static Message readFromStream(DataInputStream dis) throws IOException {
        int totalLength = dis.readInt();
        if (totalLength <= 0 || totalLength > 64 * 1024 * 1024) {
            throw new IOException("Invalid message length: " + totalLength);
        }

        byte[] data = new byte[totalLength];
        dis.readFully(data);
        return unpack(data);
    }

    /**
     * Writes the message to an output stream with length prefix.
     *
     * @param dos output stream to write to
     * @throws IOException if write fails
     */
    public void writeToStream(DataOutputStream dos) throws IOException {
        byte[] data = pack();
        dos.writeInt(data.length);
        dos.write(data);
        dos.flush();
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a REGISTER message for worker registration.
     */
    public static Message createRegister(String studentId, String workerId) {
        Message msg = new Message();
        msg.messageType = "REGISTER";
        msg.studentId = studentId;
        msg.payload = workerId.getBytes(StandardCharsets.UTF_8);
        return msg;
    }

    /**
     * Creates a REGISTER_ACK message acknowledging worker registration.
     */
    public static Message createRegisterAck(String studentId) {
        Message msg = new Message();
        msg.messageType = "REGISTER_ACK";
        msg.studentId = studentId;
        msg.payload = EMPTY_PAYLOAD;
        return msg;
    }

    /**
     * Creates an RPC_REQUEST message for task distribution.
     * Payload format: taskId;taskType;matrixData
     */
    public static Message createRpcRequest(String studentId, String taskId,
                                           String taskType, byte[] matrixData) {
        Message msg = new Message();
        msg.messageType = "RPC_REQUEST";
        msg.studentId = studentId;

        String header = taskId + ";" + taskType + ";";
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] safeMatrixData = (matrixData != null) ? matrixData : EMPTY_PAYLOAD;

        msg.payload = new byte[headerBytes.length + safeMatrixData.length];
        System.arraycopy(headerBytes, 0, msg.payload, 0, headerBytes.length);
        System.arraycopy(safeMatrixData, 0, msg.payload, headerBytes.length, safeMatrixData.length);

        return msg;
    }

    /**
     * Creates an RPC_RESPONSE message for task results.
     * Payload format: taskId;resultData
     */
    public static Message createRpcResponse(String studentId, String taskId, byte[] resultData) {
        Message msg = new Message();
        msg.messageType = "RPC_RESPONSE";
        msg.studentId = studentId;

        String header = taskId + ";";
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] safeResultData = (resultData != null) ? resultData : EMPTY_PAYLOAD;

        msg.payload = new byte[headerBytes.length + safeResultData.length];
        System.arraycopy(headerBytes, 0, msg.payload, 0, headerBytes.length);
        System.arraycopy(safeResultData, 0, msg.payload, headerBytes.length, safeResultData.length);

        return msg;
    }

    /**
     * Creates a HEARTBEAT message.
     */
    public static Message createHeartbeat(String studentId) {
        Message msg = new Message();
        msg.messageType = "HEARTBEAT";
        msg.studentId = studentId;
        msg.payload = EMPTY_PAYLOAD;
        return msg;
    }

    /**
     * Creates a HEARTBEAT_ACK message.
     */
    public static Message createHeartbeatAck(String studentId, String workerId) {
        Message msg = new Message();
        msg.messageType = "HEARTBEAT_ACK";
        msg.studentId = studentId;
        msg.payload = workerId.getBytes(StandardCharsets.UTF_8);
        return msg;
    }

    /**
     * Creates a TASK_COMPLETE message.
     */
    public static Message createTaskComplete(String studentId, String taskId) {
        Message msg = new Message();
        msg.messageType = "TASK_COMPLETE";
        msg.studentId = studentId;
        msg.payload = taskId.getBytes(StandardCharsets.UTF_8);
        return msg;
    }

    /**
     * Creates a TASK_ERROR message.
     */
    public static Message createTaskError(String studentId, String taskId, String error) {
        Message msg = new Message();
        msg.messageType = "TASK_ERROR";
        msg.studentId = studentId;
        String errorPayload = taskId + ";" + (error != null ? error : "Unknown error");
        msg.payload = errorPayload.getBytes(StandardCharsets.UTF_8);
        return msg;
    }

    // ==================== Utility Methods ====================

    /**
     * Gets the payload as a UTF-8 string.
     */
    public String getPayloadAsString() {
        return (payload != null) ? new String(payload, StandardCharsets.UTF_8) : "";
    }

    @Override
    public String toString() {
        return String.format("Message{magic='%s', version=%d, type='%s', studentId='%s', " +
                "timestamp=%d, payloadLength=%d}",
                magic, version, messageType, studentId, timestamp,
                (payload != null) ? payload.length : 0);
    }
}
