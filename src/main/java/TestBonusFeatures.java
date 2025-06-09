import java.util.ArrayList;
import java.util.List;

public class TestBonusFeatures {
    public static void main(String[] args) throws Exception {
        // 启动服务器
        new Thread(() -> {
            EasyCloudDiskServer server = new EasyCloudDiskServer();
            server.start();
        }).start();

        // 等待服务器启动
        Thread.sleep(2000);

        // 启动客户端
        EasyCloudDiskClient client = new EasyCloudDiskClient();
        client.start();

        // 测试获取文件列表（先测试简单功能）
        System.out.println("\n===== 测试获取文件列表功能 =====");
        List<EasyCloudDiskClient.FileInfo> files = client.getFileList();
        System.out.println("云盘文件列表:");
        for (EasyCloudDiskClient.FileInfo file : files) {
            System.out.println(file);
        }

        // 等待一段时间让连接完全释放
        Thread.sleep(2000);

        // 测试批量上传
        System.out.println("\n===== 测试批量上传功能 =====");
        List<String[]> uploadPaths = new ArrayList<>();
        uploadPaths.add(new String[]{"src/main/java/local/file1MB", "batch/file1.txt"});
        uploadPaths.add(new String[]{"src/main/java/local/chunk-0.txt", "batch/file2.txt"});
        client.batchUpload(uploadPaths);

        // 等待批量上传完成
        Thread.sleep(3000);

        // 测试批量下载
        System.out.println("\n===== 测试批量下载功能 =====");
        List<String[]> downloadPaths = new ArrayList<>();
        downloadPaths.add(new String[]{"batch/file1.txt", "src/main/java/local/batchDown1.txt"});
        downloadPaths.add(new String[]{"batch/file2.txt", "src/main/java/local/batchDown2.txt"});
        client.batchDownload(downloadPaths);

        // 等待批量下载完成
        Thread.sleep(3000);

        // 再次查看文件列表，验证批量上传结果
        System.out.println("\n===== 验证批量上传结果 =====");
        files = client.getFileList();
        System.out.println("更新后的云盘文件列表:");
        for (EasyCloudDiskClient.FileInfo file : files) {
            System.out.println(file);
        }

        // 等待更长时间确保所有连接都已释放
        Thread.sleep(5000);

        // 最后测试多线程下载（最容易出问题的功能）
        System.out.println("\n===== 测试多线程下载功能 =====");

        // 确保有文件可以下载，先上传一个测试文件
        client.uploadFileSingleThread("src/main/java/local/file1MB", "multitest.txt");
        Thread.sleep(2000);

        long start = System.nanoTime();

        try {
            client.downloadFileMultiThread("multitest.txt", "src/main/java/local/multiDownload.txt");
        } catch (Exception e) {
            System.err.println("多线程下载过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        }

        long end = System.nanoTime();
        double time = (end - start) / 1_000_000_000.0;
        System.out.printf("多线程下载 1MB 文件的耗时: %.3f s%n", time);

        // 等待所有连接完全关闭
        Thread.sleep(3000);

        System.out.println("\n所有测试完成！");

        // 验证下载的文件是否正确
        try {
            if (new java.io.File("src/main/java/local/multiDownload.txt").exists()) {
                System.out.println("多线程下载的文件已创建");
                // 可以进一步验证文件内容的MD5值
            } else {
                System.out.println("多线程下载的文件未找到");
            }
        } catch (Exception e) {
            System.err.println("验证下载文件时出错: " + e.getMessage());
        }
    }
}