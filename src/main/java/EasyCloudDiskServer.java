import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EasyCloudDiskServer {
    private static final int PORT = 8888;
    private static final String CLOUD_DIR = "src/main/java/cloud/";
    private static final int BUFFER_SIZE = 4096;
    private ExecutorService executorService;

    public EasyCloudDiskServer() {
        this.executorService = Executors.newFixedThreadPool(10);
    }

    /**
     * 服务端启动
     */
    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("服务器已启动，监听端口: " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("客户端已连接: " + clientSocket.getInetAddress());

                // 处理客户端请求
                executorService.execute(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("服务器错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream())
        ) {
            // 读取命令
            String command = dis.readUTF();
            System.out.println("接收到命令: " + command + " 来自: " + clientSocket.getInetAddress());

            switch (command) {
                case "UPLOAD":
                    handleUpload(dis, dos);
                    break;
                case "DOWNLOAD":
                    handleDownload(dis, dos);
                    break;
                case "LIST":
                    handleList(dos);
                    break;
                case "UPLOAD_MULTI":
                    handleMultiUpload(dis, dos);
                    break;
                case "BATCH_UPLOAD":
                    handleBatchUpload(dis, dos);
                    break;
                case "BATCH_DOWNLOAD":
                    handleBatchDownload(dis, dos);
                    break;
                case "RANGE_DOWNLOAD":
                    handleRangeDownload(dis, dos);
                    break;
                default:
                    System.out.println("未知命令: " + command);
            }
        } catch (EOFException e) {
            System.err.println("客户端断开连接: " + clientSocket.getInetAddress());
        } catch (IOException e) {
            System.err.println("处理客户端请求错误: " + e.getMessage() + " 客户端: " + clientSocket.getInetAddress());
            e.printStackTrace();
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 处理单线程文件上传
    private void handleUpload(DataInputStream dis, DataOutputStream dos) throws IOException {
        // 读取文件路径和大小
        String filePath = dis.readUTF();
        String remotePath = CLOUD_DIR + filePath;
        long fileSize = dis.readLong();
        String clientMD5 = dis.readUTF();

        System.out.println("正在接收文件: " + filePath + ", 大小: " + fileSize + " 字节");

        // 创建目录（如果需要）
        File file = new File(remotePath);
        file.getParentFile().mkdirs();

        // 接收文件内容
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            long totalBytesRead = 0;

            while (totalBytesRead < fileSize) {
                bytesRead = dis.read(buffer, 0, (int) Math.min(buffer.length, fileSize - totalBytesRead));
                if (bytesRead == -1) break;

                fos.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }
        }

        // 计算接收到的文件的MD5
        String serverMD5 = calculateMD5(remotePath);

        // 发送MD5校验结果
        boolean md5Match = serverMD5.equals(clientMD5);
        dos.writeBoolean(md5Match);

        System.out.println("文件接收完成: " + filePath + ", MD5校验: " + (md5Match ? "成功" : "失败"));
    }

    // 处理多线程文件上传
    private void handleMultiUpload(DataInputStream dis, DataOutputStream dos) throws IOException {
        String filePath = dis.readUTF();
        String remotePath = CLOUD_DIR + filePath;
        int chunkCount = dis.readInt();
        String clientMD5 = dis.readUTF();

        System.out.println("正在接收多线程上传文件: " + filePath + ", 分块数: " + chunkCount);

        // 创建目录（如果需要）
        File file = new File(remotePath);
        file.getParentFile().mkdirs();

        // 接收文件分块并合并
        try (FileOutputStream fos = new FileOutputStream(file)) {
            for (int i = 0; i < chunkCount; i++) {
                int chunkSize = dis.readInt();
                byte[] buffer = new byte[chunkSize];
                dis.readFully(buffer, 0, chunkSize);
                fos.write(buffer);
            }
        }

        // 计算接收到的文件的MD5
        String serverMD5 = calculateMD5(remotePath);

        // 发送MD5校验结果
        boolean md5Match = serverMD5.equals(clientMD5);
        dos.writeBoolean(md5Match);

        System.out.println("多线程文件接收完成: " + filePath + ", MD5校验: " + (md5Match ? "成功" : "失败"));
    }

    // 处理文件下载
    private void handleDownload(DataInputStream dis, DataOutputStream dos) throws IOException {
        String filePath = dis.readUTF();
        String remotePath = CLOUD_DIR + filePath;

        File file = new File(remotePath);

        if (!file.exists()) {
            dos.writeBoolean(false);
            return;
        }

        dos.writeBoolean(true);

        // 发送文件大小
        long fileSize = file.length();
        dos.writeLong(fileSize);

        // 计算并发送MD5
        String md5 = calculateMD5(remotePath);
        dos.writeUTF(md5);

        // 发送文件内容
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            long totalSent = 0;

            try {
                while ((bytesRead = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, bytesRead);
                    totalSent += bytesRead;

                    // 每发送一定量的数据就刷新缓冲区，避免缓冲区溢出
                    if (totalSent % (BUFFER_SIZE * 10) == 0) {
                        dos.flush();
                    }
                }
                dos.flush();
                System.out.println("文件发送完成: " + filePath);
            } catch (IOException e) {
                System.err.println("文件发送过程中发生错误: " + e.getMessage() + " (已发送 " + totalSent + "/" + fileSize + " 字节)");
                // 不再向上层抛出异常，这样可以避免整个handleClient方法失败
                // 只是记录错误并返回
                return;
            }
        }
    }

    // 处理文件列表请求
    private void handleList(DataOutputStream dos) throws IOException {
        try {
            File cloudDir = new File(CLOUD_DIR);
            List<String> fileList = new ArrayList<>();
            List<Long> fileSizes = new ArrayList<>();

            listFilesRecursively(cloudDir, fileList, fileSizes, CLOUD_DIR);

            // 发送文件数量
            dos.writeInt(fileList.size());

            // 发送文件路径和大小
            for (int i = 0; i < fileList.size(); i++) {
                dos.writeUTF(fileList.get(i));
                dos.writeLong(fileSizes.get(i));
                dos.flush(); // 每发送一个文件信息就刷新缓冲区
            }

            System.out.println("发送文件列表完成，共 " + fileList.size() + " 个文件");

            // 我们不需要等待客户端的确认
            // 客户端可以选择关闭连接，服务器会在handleClient方法中捕获这个异常
        } catch (IOException e) {
            System.err.println("处理文件列表请求错误: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // 递归列出目录中的所有文件
    private void listFilesRecursively(File dir, List<String> fileList, List<Long> fileSizes, String basePath) {
        File[] files = dir.listFiles();
        if (files == null) return;

        for (File file : files) {
            if (file.isFile()) {
                String relativePath = file.getAbsolutePath().substring(basePath.length());
                fileList.add(relativePath);
                fileSizes.add(file.length());
            } else if (file.isDirectory()) {
                listFilesRecursively(file, fileList, fileSizes, basePath);
            }
        }
    }

    // 处理批量上传
    private void handleBatchUpload(DataInputStream dis, DataOutputStream dos) throws IOException {
        int fileCount = dis.readInt();
        System.out.println("接收批量上传请求，文件数量: " + fileCount);

        try {
            for (int i = 0; i < fileCount; i++) {
                // 读取文件路径和大小
                String filePath = dis.readUTF();
                String remotePath = CLOUD_DIR + filePath;
                long fileSize = dis.readLong();
                String clientMD5 = dis.readUTF();

                System.out.println("正在接收批量文件[" + (i + 1) + "/" + fileCount + "]: " + filePath + ", 大小: " + fileSize + " 字节");

                // 创建目录（如果需要）
                File file = new File(remotePath);
                file.getParentFile().mkdirs();

                // 接收文件内容
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    long totalBytesRead = 0;

                    while (totalBytesRead < fileSize) {
                        bytesRead = dis.read(buffer, 0, (int) Math.min(buffer.length, fileSize - totalBytesRead));
                        if (bytesRead == -1) break;

                        fos.write(buffer, 0, bytesRead);
                        totalBytesRead += bytesRead;
                    }
                }

                // 计算接收到的文件的MD5
                String serverMD5 = calculateMD5(remotePath);

                // 发送MD5校验结果
                boolean md5Match = serverMD5.equals(clientMD5);
                dos.writeBoolean(md5Match);

                System.out.println("批量文件[" + (i + 1) + "/" + fileCount + "]接收完成: " + filePath + ", MD5校验: " + (md5Match ? "成功" : "失败"));
            }

            System.out.println("批量上传完成，共 " + fileCount + " 个文件");
        } catch (IOException e) {
            System.err.println("批量上传处理错误: " + e.getMessage());
            e.printStackTrace();
            throw e;  // 重新抛出异常，让上层处理
        }
    }

    // 处理批量下载
    private void handleBatchDownload(DataInputStream dis, DataOutputStream dos) throws IOException {
        int fileCount = dis.readInt();
        System.out.println("接收批量下载请求，文件数量: " + fileCount);

        try {
            for (int i = 0; i < fileCount; i++) {
                // 读取文件路径
                String filePath = dis.readUTF();
                String remotePath = CLOUD_DIR + filePath;
                File file = new File(remotePath);

                System.out.println("处理批量下载文件[" + (i + 1) + "/" + fileCount + "]: " + filePath);

                // 检查文件是否存在
                if (!file.exists() || !file.isFile()) {
                    dos.writeBoolean(false);
                    System.out.println("文件不存在: " + remotePath);
                    continue;
                }

                dos.writeBoolean(true);

                // 发送文件大小
                long fileSize = file.length();
                dos.writeLong(fileSize);

                // 计算并发送MD5
                String md5 = calculateMD5(remotePath);
                dos.writeUTF(md5);

                // 发送文件内容
                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    long totalSent = 0;

                    try {
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            dos.write(buffer, 0, bytesRead);
                            totalSent += bytesRead;

                            // 每发送一定量的数据就刷新缓冲区，避免缓冲区溢出
                            if (totalSent % (BUFFER_SIZE * 10) == 0) {
                                dos.flush();
                            }
                        }
                        dos.flush();
                        System.out.println("批量文件[" + (i + 1) + "/" + fileCount + "]发送完成: " + filePath);
                    } catch (IOException e) {
                        System.err.println("批量文件[" + (i + 1) + "/" + fileCount + "]发送过程中发生错误: " + e.getMessage() + " (已发送 " + totalSent + "/" + fileSize + " 字节)");
                        // 如果一个文件发送失败，尝试继续发送下一个文件
                        continue;
                    }
                }
            }

            System.out.println("批量下载完成，共 " + fileCount + " 个文件");
        } catch (IOException e) {
            System.err.println("批量下载处理错误: " + e.getMessage());
            e.printStackTrace();
            // 不再向上抛出异常
        }
    }

    // 处理范围下载 (用于多线程下载)
    private void handleRangeDownload(DataInputStream dis, DataOutputStream dos) throws IOException {
        String filePath = dis.readUTF();
        String remotePath = CLOUD_DIR + filePath;
        long startPos = dis.readLong();
        long length = dis.readLong();

        File file = new File(remotePath);

        if (!file.exists() || startPos >= file.length()) {
            System.err.println("范围下载错误: 文件不存在或起始位置无效");
            return;
        }

        // 读取指定范围的文件内容并发送
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(startPos);

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            long remaining = length;
            long totalSent = 0;

            try {
                while (remaining > 0) {
                    bytesRead = raf.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (bytesRead == -1) break;

                    dos.write(buffer, 0, bytesRead);
                    remaining -= bytesRead;
                    totalSent += bytesRead;

                    // 每发送一定量的数据就刷新缓冲区，避免缓冲区溢出
                    if (totalSent % (BUFFER_SIZE * 10) == 0) {
                        dos.flush();
                    }
                }
                dos.flush();
                System.out.println("范围下载完成: " + filePath + ", 位置: " + startPos + ", 长度: " + length);
            } catch (IOException e) {
                System.err.println("范围下载过程中发生错误: " + e.getMessage() +
                                 " (已发送 " + totalSent + "/" + length + " 字节, 位置: " + startPos + ")");
                // 不再向上层抛出异常，这样可以避免整个handleClient方法失败
                return;
            }
        }
    }

    // 计算文件的MD5值
    private String calculateMD5(String filePath) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (FileInputStream fis = new FileInputStream(filePath)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;

                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xFF));
            }

            return sb.toString();
        } catch (IOException | NoSuchAlgorithmException e) {
            System.err.println("计算MD5失败: " + e.getMessage());
            e.printStackTrace();
            return "";
        }
    }
}
