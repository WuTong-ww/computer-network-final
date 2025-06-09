import util.MD5Util;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EasyCloudDiskClient {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8888;
    private static final int BUFFER_SIZE = 4096;
    private static final int THREAD_COUNT = 5; // 多线程上传的线程数

    /**
     * 启动客户端
     */
    public void start() {
        System.out.println("客户端已启动，连接到服务器: " + SERVER_ADDRESS + ":" + SERVER_PORT);
    }

    /**
     * 单线程上传文件
     *
     * @param localFilePath  本地文件路径
     * @param remoteFilePath 云盘文件路径
     */
    public void uploadFileSingleThread(String localFilePath, String remoteFilePath) {
        File localFile = new File(localFilePath);
        if (!localFile.exists() || !localFile.isFile()) {
            System.err.println("本地文件不存在或不是一个文件: " + localFilePath);
            return;
        }

        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // 发送上传命令
            dos.writeUTF("UPLOAD");

            // 发送文件信息
            dos.writeUTF(remoteFilePath);
            dos.writeLong(localFile.length());

            // 计算并发送文件MD5
            String md5 = MD5Util.calculateMD5(localFilePath);
            dos.writeUTF(md5);

            // 发送文件内容
            try (FileInputStream fis = new FileInputStream(localFile)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;

                while ((bytesRead = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, bytesRead);
                }
                dos.flush();
            }

            // 接收MD5校验结果
            boolean md5Match = dis.readBoolean();
            if (md5Match) {
                System.out.println("文件上传成功: " + localFilePath + " -> " + remoteFilePath);
            } else {
                System.err.println("文件上传失败，MD5校验不匹配: " + localFilePath);
            }

        } catch (IOException e) {
            System.err.println("上传文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 多线程上传文件
     *
     * @param localFilePath  本地文件路径
     * @param remoteFilePath 云盘文件路径
     */
    public void uploadFileMultiThread(String localFilePath, String remoteFilePath) {
        File localFile = new File(localFilePath);
        if (!localFile.exists() || !localFile.isFile()) {
            System.err.println("本地文件不存在或不是一个文件: " + localFilePath);
            return;
        }

        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // 发送多线程上传命令
            dos.writeUTF("UPLOAD_MULTI");

            // 发送文件路径
            dos.writeUTF(remoteFilePath);

            // 计算MD5
            String md5 = MD5Util.calculateMD5(localFilePath);

            // 分块上传
            long fileSize = localFile.length();
            int chunkSize = (int) Math.ceil((double) fileSize / THREAD_COUNT);
            List<byte[]> chunks = new ArrayList<>();

            // 使用线程池并行读取文件块
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<byte[]>> futures = new ArrayList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                final int chunkIndex = i;
                final long startPos = (long) chunkIndex * chunkSize;
                final long endPos = Math.min(startPos + chunkSize, fileSize);

                Future<byte[]> future = executor.submit(() -> {
                    byte[] data = new byte[(int) (endPos - startPos)];
                    try (RandomAccessFile raf = new RandomAccessFile(localFile, "r")) {
                        raf.seek(startPos);
                        raf.readFully(data);
                    }
                    return data;
                });

                futures.add(future);
            }

            // 收集所有块
            for (Future<byte[]> future : futures) {
                chunks.add(future.get());
            }

            executor.shutdown();

            // 发送块数量和MD5
            dos.writeInt(chunks.size());
            dos.writeUTF(md5);

            // 发送所有块
            for (byte[] chunk : chunks) {
                dos.writeInt(chunk.length);
                dos.write(chunk);
            }
            dos.flush();

            // 接收MD5校验结果
            boolean md5Match = dis.readBoolean();
            if (md5Match) {
                System.out.println("多线程文件上传成功: " + localFilePath + " -> " + remoteFilePath);
            } else {
                System.err.println("多线程文件上传失败，MD5校验不匹配: " + localFilePath);
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("多线程上传文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 下载文件
     *
     * @param remoteFilePath 云盘文件路径
     * @param localFilePath  本地文件路径
     */
    public void downloadFile(String remoteFilePath, String localFilePath) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // 发送下载命令
            dos.writeUTF("DOWNLOAD");
            dos.writeUTF(remoteFilePath);

            // 检查文件是否存在
            boolean fileExists = dis.readBoolean();
            if (!fileExists) {
                System.err.println("云盘文件不存在: " + remoteFilePath);
                return;
            }

            // 获取文件大小和MD5
            long fileSize = dis.readLong();
            String serverMD5 = dis.readUTF();

            // 创建目录（如果需要）
            File localFile = new File(localFilePath);
            localFile.getParentFile().mkdirs();

            // 接收文件内容
            try (FileOutputStream fos = new FileOutputStream(localFile)) {
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

            // 验证MD5
            String clientMD5 = MD5Util.calculateMD5(localFilePath);
            if (serverMD5.equals(clientMD5)) {
                System.out.println("文件下载成功: " + remoteFilePath + " -> " + localFilePath);
            } else {
                System.err.println("文件下载失败，MD5校验不匹配: " + remoteFilePath);
            }

        } catch (IOException e) {
            System.err.println("下载文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Bonus:多线程下载文件
     *
     * @param remoteFilePath 云盘文件路径
     * @param localFilePath  本地文件路径
     */
    public void downloadFileMultiThread(String remoteFilePath, String localFilePath) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT)) {
            socket.setSoTimeout(10000); // 设置10秒超时

            try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                 DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

                // 首先获取文件信息
                dos.writeUTF("DOWNLOAD");
                dos.writeUTF(remoteFilePath);

                // 检查文件是否存在
                boolean fileExists = dis.readBoolean();
                if (!fileExists) {
                    System.err.println("云盘文件不存在: " + remoteFilePath);
                    return;
                }

                // 获取文件大小和MD5
                long fileSize = dis.readLong();
                String serverMD5 = dis.readUTF();

                // 读取完文件信息后关闭这个连接
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;
                while (dis.available() > 0) {
                    dis.read(buffer);
                }
            }

            // 创建目录（如果需要）
            File localFile = new File(localFilePath);
            localFile.getParentFile().mkdirs();

            // 计算每个线程下载的块大小
            long fileSize = new File(localFilePath).length();
            int chunkSize = (int) Math.ceil((double) fileSize / THREAD_COUNT);

            // 准备临时文件
            File[] tempFiles = new File[THREAD_COUNT];
            for (int i = 0; i < THREAD_COUNT; i++) {
                tempFiles[i] = new File(localFilePath + ".part" + i);
            }

            // 使用线程池并行下载文件块
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                final int chunkIndex = i;
                final long startPos = (long) chunkIndex * chunkSize;
                final long endPos = Math.min(startPos + chunkSize, fileSize);
                final File tempFile = tempFiles[chunkIndex];

                Future<?> future = executor.submit(() -> {
                    Socket chunkSocket = null;
                    try {
                        chunkSocket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                        chunkSocket.setSoTimeout(10000); // 设置10秒超时

                        try (DataInputStream chunkDis = new DataInputStream(chunkSocket.getInputStream());
                             DataOutputStream chunkDos = new DataOutputStream(chunkSocket.getOutputStream())) {

                            // 发送范围下载请求
                            chunkDos.writeUTF("RANGE_DOWNLOAD");
                            chunkDos.writeUTF(remoteFilePath);
                            chunkDos.writeLong(startPos);
                            chunkDos.writeLong(endPos - startPos);

                            // 接收数据块
                            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                                byte[] chunkBuffer = new byte[BUFFER_SIZE];
                                int chunkBytesRead;
                                long remaining = endPos - startPos;

                                while (remaining > 0) {
                                    chunkBytesRead = chunkDis.read(chunkBuffer, 0, (int) Math.min(chunkBuffer.length, remaining));
                                    if (chunkBytesRead == -1) break;

                                    fos.write(chunkBuffer, 0, chunkBytesRead);
                                    remaining -= chunkBytesRead;
                                }
                            }

                            System.out.println("下载分块 " + chunkIndex + " 完成: 位置 " + startPos + "-" + (endPos-1));
                        }
                    } catch (IOException e) {
                        System.err.println("下载分块 " + chunkIndex + " 错误: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        // 确保Socket被关闭
                        if (chunkSocket != null && !chunkSocket.isClosed()) {
                            try {
                                chunkSocket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });

                futures.add(future);
            }

            // 等待所有下载任务完成
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("等待下载任务完成时出错: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            executor.shutdown();

            // 合并所有分块
            try (FileOutputStream fos = new FileOutputStream(localFile)) {
                for (int i = 0; i < THREAD_COUNT; i++) {
                    File tempFile = tempFiles[i];
                    if (tempFile.exists()) {
                        try (FileInputStream fis = new FileInputStream(tempFile)) {
                            byte[] mergeBuffer = new byte[BUFFER_SIZE];
                            int mergeBytesRead;

                            while ((mergeBytesRead = fis.read(mergeBuffer)) != -1) {
                                fos.write(mergeBuffer, 0, mergeBytesRead);
                            }
                        }
                        // 删除临时文件
                        tempFile.delete();
                    }
                }
            }

            // 验证MD5
            String clientMD5 = MD5Util.calculateMD5(localFilePath);
            String serverMD5 = ""; // 从之前的连接中获取

            // 再次连接以获取MD5（如果需要）
            try (Socket verifySocket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                 DataInputStream verifyDis = new DataInputStream(verifySocket.getInputStream());
                 DataOutputStream verifyDos = new DataOutputStream(verifySocket.getOutputStream())) {

                verifyDos.writeUTF("DOWNLOAD");
                verifyDos.writeUTF(remoteFilePath);

                boolean exists = verifyDis.readBoolean();
                if (exists) {
                    verifyDis.readLong(); // 跳过文件大小
                    serverMD5 = verifyDis.readUTF(); // 获取MD5
                }
            }

            if (serverMD5.equals(clientMD5)) {
                System.out.println("多线程文件下载成功: " + remoteFilePath + " -> " + localFilePath);
            } else {
                System.err.println("多线程文件下载失败，MD5校验不匹配: " + remoteFilePath);
            }

            // 在完成后确保所有连接都已关闭
            System.out.println("多线程下载完成，所有连接已关闭");
            // 给系统一些时间来释放资源
            Thread.sleep(500);

        } catch (IOException | InterruptedException e) {
            System.err.println("多线程下载文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取云盘文件列表
     *
     * @return 文件路径和大小的列表
     */
    public List<FileInfo> getFileList() {
        List<FileInfo> fileList = new ArrayList<>();
        int maxRetries = 3; // 最大重试次数
        int retryCount = 0;
        boolean success = false;

        while (!success && retryCount < maxRetries) {
            try {
                if (retryCount > 0) {
                    System.out.println("正在尝试重新获取文件列表，第 " + retryCount + " 次重试...");
                    // 在重试前等待一段时间
                    Thread.sleep(1000);
                }

                // 创建新的连接
                try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                     DataInputStream dis = new DataInputStream(socket.getInputStream());
                     DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

                    socket.setSoTimeout(5000); // 设置更短的超时时间

                    // 发送列表命令
                    dos.writeUTF("LIST");
                    dos.flush();

                    // 接收文件数量
                    int fileCount = dis.readInt();
                    System.out.println("服务器返回文件数量: " + fileCount);

                    // 接收文件信息
                    for (int i = 0; i < fileCount; i++) {
                        String filePath = dis.readUTF();
                        long fileSize = dis.readLong();
                        fileList.add(new FileInfo(filePath, fileSize));
                    }

                    success = true; // 如果没有异常，则标记为成功
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("获取文件列表错误: " + e.getMessage());
                retryCount++;
                if (retryCount >= maxRetries) {
                    System.err.println("达到最大重试次数，无法获取文件列表");
                    e.printStackTrace();
                }
                // 清空已获取的文件列表，准备重试
                fileList.clear();
            }
        }

        return fileList;
    }

    /**
     * 批量上传文件
     *
     * @param filePaths 本地文件路径和远程文件路径的映射
     */
    public void batchUpload(List<String[]> filePaths) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // 发送批量上传命令
            dos.writeUTF("BATCH_UPLOAD");
            dos.writeInt(filePaths.size());

            for (String[] pathPair : filePaths) {
                String localPath = pathPair[0];
                String remotePath = pathPair[1];

                File localFile = new File(localPath);
                if (!localFile.exists() || !localFile.isFile()) {
                    System.err.println("本地文件不存在或不是一个文件: " + localPath);
                    continue;
                }

                // 发送文件信息
                dos.writeUTF(remotePath);
                dos.writeLong(localFile.length());

                // 计算并发送文件MD5
                String md5 = MD5Util.calculateMD5(localPath);
                dos.writeUTF(md5);

                // 发送文件内容
                try (FileInputStream fis = new FileInputStream(localFile)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;

                    while ((bytesRead = fis.read(buffer)) != -1) {
                        dos.write(buffer, 0, bytesRead);
                    }
                }

                // 接收MD5校验结果
                boolean md5Match = dis.readBoolean();
                if (md5Match) {
                    System.out.println("文件上传成功: " + localPath + " -> " + remotePath);
                } else {
                    System.err.println("文件上传失败，MD5校验不匹配: " + localPath);
                }
            }

        } catch (IOException e) {
            System.err.println("批量上传文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 批量下载文件
     *
     * @param filePaths 远程文件路径和本地文件路径的映射
     */
    public void batchDownload(List<String[]> filePaths) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            // 发送批量下载命令
            dos.writeUTF("BATCH_DOWNLOAD");
            dos.writeInt(filePaths.size());

            for (String[] pathPair : filePaths) {
                String remotePath = pathPair[0];
                String localPath = pathPair[1];

                // 发送文件路径
                dos.writeUTF(remotePath);

                // 检查文件是否存在
                boolean fileExists = dis.readBoolean();
                if (!fileExists) {
                    System.err.println("云盘文件不存在: " + remotePath);
                    continue;
                }

                // 获取文件大小和MD5
                long fileSize = dis.readLong();
                String serverMD5 = dis.readUTF();

                // 创建目录（如果需要）
                File localFile = new File(localPath);
                localFile.getParentFile().mkdirs();

                // 接收文件内容
                try (FileOutputStream fos = new FileOutputStream(localFile)) {
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

                // 验证MD5
                String clientMD5 = MD5Util.calculateMD5(localPath);
                if (serverMD5.equals(clientMD5)) {
                    System.out.println("文件下载成功: " + remotePath + " -> " + localPath);
                } else {
                    System.err.println("文件下载失败，MD5校验不匹配: " + remotePath);
                }
            }

        } catch (IOException e) {
            System.err.println("批量下载文件错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取服务器地址
     * @return 服务器地址
     */
    public static String getServerAddress() {
        return SERVER_ADDRESS;
    }

    /**
     * 获取服务器端口
     * @return 服务器端口
     */
    public static int getServerPort() {
        return SERVER_PORT;
    }

    /**
     * 文件信息类
     */
    public static class FileInfo {
        private String filePath;
        private long fileSize;

        public FileInfo(String filePath, long fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public long getFileSize() {
            return fileSize;
        }

        @Override
        public String toString() {
            return filePath + " (" + formatFileSize(fileSize) + ")";
        }

        private String formatFileSize(long size) {
            if (size < 1024) {
                return size + " B";
            } else if (size < 1024 * 1024) {
                return String.format("%.2f KB", size / 1024.0);
            } else if (size < 1024 * 1024 * 1024) {
                return String.format("%.2f MB", size / (1024.0 * 1024));
            } else {
                return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
            }
        }
    }
}
