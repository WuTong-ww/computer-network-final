import java.util.ArrayList;
import java.util.List;
import java.net.Socket;

public class TestBonusFeatures {
    public static void main(String[] args) throws Exception {
        // 启动服务器
        new Thread(() -> {
            EasyCloudDiskServer server = new EasyCloudDiskServer();
            server.start();
        }).start();

        // 等待服务器启动
        Thread.sleep(1000);

        // 启动客户端
        EasyCloudDiskClient client = new EasyCloudDiskClient();
        client.start();

        // 测试多线程下载
        System.out.println("\n===== 测试多线程下载功能 =====");
        long start = System.nanoTime();

        client.downloadFileMultiThread("file1MB", "src/main/java/local/multiDownload.txt");

        long end = System.nanoTime();
        double time = (end - start) / 1_000_000_000.0;
        System.out.printf("多线程下载 1MB 文件的耗时: %.3f s%n", time);
        // 等待所有连接完全关闭
        Thread.sleep(3000);

        // 给网络连接一个重置的机会
        try {
            System.out.println("正在重置网络连接...");
            // 创建一个临时连接然后立即关闭，帮助清理可能存在的旧连接
            Socket resetSocket = new Socket(EasyCloudDiskClient.getServerAddress(), EasyCloudDiskClient.getServerPort());
            resetSocket.close();
        } catch (Exception e) {
            System.out.println("网络连接重置过程中发生异常（这通常是可接受的）: " + e.getMessage());
        }

        // 再等待一段时间
        Thread.sleep(1000);

        // 测试获取文件列表
        System.out.println("\n===== 测试获取文件列表功能 =====");
        List<EasyCloudDiskClient.FileInfo> files = client.getFileList();
        System.out.println("云盘文件列表:");
        for (EasyCloudDiskClient.FileInfo file : files) {
            System.out.println(file);
        }

        // 测试批量上传
        System.out.println("\n===== 测试批量上传功能 =====");
        List<String[]> uploadPaths = new ArrayList<>();
        uploadPaths.add(new String[]{"src/main/java/local/file1MB", "batch/file1.txt"});
        uploadPaths.add(new String[]{"src/main/java/local/chunk-0.txt", "batch/file2.txt"});
        client.batchUpload(uploadPaths);

        // 测试批量下载
        System.out.println("\n===== 测试批量下载功能 =====");
        List<String[]> downloadPaths = new ArrayList<>();
        downloadPaths.add(new String[]{"batch/file1.txt", "src/main/java/local/batchDown1.txt"});
        downloadPaths.add(new String[]{"batch/file2.txt", "src/main/java/local/batchDown2.txt"});
        client.batchDownload(downloadPaths);

        // 再次查看文件列表，验证批量上传结果
        System.out.println("\n===== 验证批量上传结果 =====");
        files = client.getFileList();
        System.out.println("更新后的云盘文件列表:");
        for (EasyCloudDiskClient.FileInfo file : files) {
            System.out.println(file);
        }

        System.out.println("\n所有测试完成！");
    }
}
