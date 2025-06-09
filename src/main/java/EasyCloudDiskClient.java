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
    private static final int CONNECTION_TIMEOUT = 10000; // 连接超时时间
    private static final int READ_TIMEOUT = 15000; // 读取超时时间

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

            socket.setSoTimeout(READ_TIMEOUT);

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

            socket.setSoTimeout(READ_TIMEOUT);

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

            socket.setSoTimeout(READ_TIMEOUT);

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
     * Bonus:多线程下载文件 - 修复版本
     *
     * @param remoteFilePath 云盘文件路径
     * @param localFilePath  本地文件路径
     */
    public void downloadFileMultiThread(String remoteFilePath, String localFilePath) {
        // 首先获取文件信息
        long fileSize;
        String serverMD5;

        try (Socket infoSocket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             DataInputStream infoDis = new DataInputStream(infoSocket.getInputStream());
             DataOutputStream infoDos = new DataOutputStream(infoSocket.getOutputStream())) {

            infoSocket.setSoTimeout(READ_TIMEOUT);

            // 获取文件信息
            infoDos.writeUTF("DOWNLOAD");
            infoDos.writeUTF(remoteFilePath);

            boolean fileExists = infoDis.readBoolean();
            if (!fileExists) {
                System.err.println("云盘文件不存在: " + remoteFilePath);
                return;
            }

            fileSize = infoDis.readLong();
            serverMD5 = infoDis.readUTF();

            // 读取并丢弃文件内容，因为我们只需要文件信息
            byte[] buffer = new byte[BUFFER_SIZE];
            while (infoDis.available() > 0) {
                infoDis.read(buffer);
            }

        } catch (IOException e) {
            System.err.println("获取文件信息失败: " + e.getMessage());
            return;
        }

        // 创建目录（如果需要）
        File localFile = new File(localFilePath);
        localFile.getParentFile().mkdirs();

        // 计算每个线程下载的块大小
        long chunkSize = fileSize / THREAD_COUNT;
        if (fileSize % THREAD_COUNT != 0) {
            chunkSize++;
        }

        // 准备临时文件
        File[] tempFiles = new File[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            tempFiles[i] = new File(localFilePath + ".part" + i);
        }

        // 使用线程池并行下载文件块，但限制并发连接数
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(THREAD_COUNT, 3)); // 限制最大并发连接数为3
        List<Future<Boolean>> futures = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int chunkIndex = i;
            final long startPos = (long) chunkIndex * chunkSize;
            final long endPos = Math.min(startPos + chunkSize, fileSize);
            final File tempFile = tempFiles[chunkIndex];

            if (startPos >= fileSize) {
                latch.countDown();
                continue;
            }

            Future<Boolean> future = executor.submit(() -> {
                boolean success = false;
                int retryCount = 0;
                int maxRetries = 3;

                while (!success && retryCount < maxRetries) {
                    try {
                        // 添加重试间隔
                        if (retryCount > 0) {
                            Thread.sleep(1000 * retryCount);
                        }

                        Socket chunkSocket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                        chunkSocket.setSoTimeout(READ_TIMEOUT);
                        chunkSocket.setTcpNoDelay(true);

                        try (DataInputStream chunkDis = new DataInputStream(chunkSocket.getInputStream());
                             DataOutputStream chunkDos = new DataOutputStream(chunkSocket.getOutputStream());
                             FileOutputStream fos = new FileOutputStream(tempFile)) {

                            // 发送范围下载请求
                            chunkDos.writeUTF("RANGE_DOWNLOAD");
                            chunkDos.writeUTF(remoteFilePath);
                            chunkDos.writeLong(startPos);
                            chunkDos.writeLong(endPos - startPos);
                            chunkDos.flush();

                            // 接收数据块
                            byte[] chunkBuffer = new byte[BUFFER_SIZE];
                            int chunkBytesRead;
                            long remaining = endPos - startPos;

                            while (remaining > 0) {
                                chunkBytesRead = chunkDis.read(chunkBuffer, 0, (int) Math.min(chunkBuffer.length, remaining));
                                if (chunkBytesRead == -1) break;

                                fos.write(chunkBuffer, 0, chunkBytesRead);
                                remaining -= chunkBytesRead;
                            }

                            success = true;
                            System.out.println("下载分块 " + chunkIndex + " 完成: 位置 " + startPos + "-" + (endPos-1));
                        } finally {
                            try {
                                chunkSocket.close();
                            } catch (IOException e) {
                                // 忽略关闭异常
                            }
                        }
                    } catch (Exception e) {
                        retryCount++;
                        System.err.println("下载分块 " + chunkIndex + " 失败 (重试 " + retryCount + "/" + maxRetries + "): " + e.getMessage());
                        if (retryCount >= maxRetries) {
                            System.err.println("分块 " + chunkIndex + " 下载最终失败");
                        }
                    }
                }

                latch.countDown();
                return success;
            });

            futures.add(future);
        }

        // 等待所有下载任务完成
        try {
            latch.await(60, TimeUnit.SECONDS); // 设置最大等待时间为60秒
        } catch (InterruptedException e) {
            System.err.println("等待下载任务完成时被中断: " + e.getMessage());
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        // 检查所有任务是否成功
        boolean allSuccess = true;
        for (Future<Boolean> future : futures) {
            try {
                if (!future.get()) {
                    allSuccess = false;
                }
            } catch (Exception e) {
                allSuccess = false;
                System.err.println("获取下载任务结果时出错: " + e.getMessage());
            }
        }

        if (!allSuccess) {
            System.err.println("部分分块下载失败，取消合并操作");
            return;
        }

        // 合并所有分块
        try (FileOutputStream fos = new FileOutputStream(localFile)) {
            for (int i = 0; i < THREAD_COUNT; i++) {
                File tempFile = tempFiles[i];
                if (tempFile.exists() && tempFile.length() > 0) {
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
        } catch (IOException e) {
            System.err.println("合并文件失败: " + e.getMessage());
            return;
        }

        // 验证MD5
        try {
            String clientMD5 = MD5Util.calculateMD5(localFilePath);
            if (serverMD5.equals(clientMD5)) {
                System.out.println("多线程文件下载成功: " + remoteFilePath + " -> " + localFilePath);
            } else {
                System.err.println("多线程文件下载失败，MD5校验不匹配: " + remoteFilePath);
            }
        } catch (Exception e) {
            System.err.println("MD5校验失败: " + e.getMessage());
        }
    }

    /**
     * 获取云盘文件列表
     *
     * @return 文件路径和大小的列表
     */
    public List<FileInfo> getFileList() {
        List<FileInfo> fileList = new ArrayList<>();
        int maxRetries = 3; // 减少重试次数
        int retryCount = 0;
        boolean success = false;

        while (!success && retryCount < maxRetries) {
            Socket socket = null;
            try {
                if (retryCount > 0) {
                    System.out.println("正在尝试重新获取文件列表，第 " + retryCount + " 次重试...");
                    Thread.sleep(2000 * retryCount); // 增加重试间隔
                }

                socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                socket.setSoTimeout(READ_TIMEOUT);
                socket.setTcpNoDelay(true);

                try (DataInputStream dis = new DataInputStream(socket.getInputStream());
                     DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

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

                    success = true;
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("获取文件列表错误: " + e.getMessage());
                retryCount++;
                if (retryCount >= maxRetries) {
                    System.err.println("达到最大重试次数，无法获取文件列表");
                    e.printStackTrace();
                }
                fileList.clear();
            } finally {
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // 忽略关闭异常
                    }
                }
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

            socket.setSoTimeout(READ_TIMEOUT * 2); // 批量操作需要更长的超时时间

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

            socket.setSoTimeout(READ_TIMEOUT * 2); // 批量操作需要更长的超时时间

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