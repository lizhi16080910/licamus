/**
 * @Title: FileUtil.java
 * @Package com.fastweb.cdnlog.bandcheck.util
 * @author LiFuqiang
 * @date 2016年7月13日 下午3:08:55
 * @version V1.0
 * @Description: TODO(用一句话描述该文件做什么)
 * Update Logs:
 * **************************************************** * Name: * Date: * Description: ******************************************************
 */
package com.fastweb.cdnlog.bigdata.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author LiFuqiang
 * @ClassName: FileUtil
 * @date 2016年7月13日 下午3:08:55
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class FileUtil {
    private static final Log LOG = LogFactory.getLog(FileUtil.class);

    /**
     * @param args
     * @throws Exception
     * @throws
     * @Title: main
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) throws Exception {

    }

    public static void listToFile(File file, List<String> list, String charSet) {

        File parentDir = new File(file.getAbsolutePath()).getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        Writer writer = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            writer = new OutputStreamWriter(fos, charSet);

            for (String str : list) {
                writer.write(str);
                writer.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void listToFile(String path, List<String> list, String charSet) {
        listToFile(new File(path), list, charSet);
    }

    public static List<String> readFile(String path, String charSet) {
        return readFile(new File(path), charSet);
    }

    public static List<String> readFile(File file, String charSet) {
        List<String> list = new ArrayList<>();
        // String encode = getFileEncode(path);
        // System.out.println(encode);
        FileInputStream fInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader buffreader = null;
        try {
            fInputStream = new FileInputStream(file);
            inputStreamReader = new InputStreamReader(fInputStream, charSet);
            buffreader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = buffreader.readLine()) != null) {
                list.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fInputStream != null) {
                    fInputStream.close();
                }
                if (inputStreamReader != null) {
                    inputStreamReader.close();
                }
                if (buffreader != null) {
                    buffreader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return list;
    }

    public static List<String> readFile(File file) {
        return readFile(file, "utf8");
    }

    public static List<String> readFile(String path) {
        return readFile(path, "utf8");
    }

    public static void listToFile(String path, List<String> list) {
        listToFile(path, list, "utf8");
    }

    public static void writeListToHdfsFile(FileSystem fs, Path path, List<String> list) throws IOException {
        OutputStream out = fs.create(path);
        for (String str : list) {
            out.write(str.getBytes());
            out.write("\n".getBytes());
        }
        if (out != null) {
            out.close();
        }
    }

    public static List<String> readHdfsFile(String path) throws IOException {
        Configuration conf = new Configuration();
        return readHdfsFile(conf, new Path(path), true);
    }

    public static List<String> readHdfsFile(Configuration conf, Path path, boolean ifFsClose) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return readHdfsFile(fs, path, ifFsClose);
    }

    /**
     * @param fs
     * @param path
     * @return
     * @throws
     * @Title: readHdfsFile
     * @Description: 传入的fs参数并不会关闭，需要另外写命令关闭fs参数
     */
    public static List<String> readHdfsFile(FileSystem fs, Path path, boolean ifFsClose) {
        List<String> list = new ArrayList<>();
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;
        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ifFsClose) {
                    if (fs != null) {
                        fs.close();
                    }
                }
                if (reader != null) {
                    reader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static List<String> readHdfsFile(Path path) throws IOException {
        Configuration conf = new Configuration();
        return readHdfsFile(conf, path, true);
    }

    public static List<String> readHdfsFile(Configuration conf, String path) throws IOException {
        return readHdfsFile(conf, new Path(path), true);
    }

    public static List<String> readDomains(String path) throws IOException {
        List<String> domains = new ArrayList<>();
        FileReader fileReader = new FileReader(new File(path));
        BufferedReader bufReader = new BufferedReader(fileReader);
        String line = null;
        while ((line = bufReader.readLine()) != null) {
            domains.add(line.trim());
        }
        bufReader.close();
        fileReader.close();
        return domains;
    }

    public static boolean ifHdfsFileExist(String path) {
        return ifHdfsFileExist(new Path(path));
    }

    public static boolean ifHdfsFileExist(Path path) {
        Configuration conf = new Configuration();
        // conf.set("fs.default.name", "hdfs://192.168.100.202:8020");
        FileSystem fs = null;
        boolean ifExist = false;
        try {
            fs = FileSystem.get(conf);
            if (fs.exists(path)) {
                ifExist = true;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            ifExist = false;
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
        return ifExist;
    }




    /**
     * @param path
     * @param fileSeparator
     * @return 判断传入的路径最后一个字符是否是分隔符，不是的话，添加分隔符
     */
    public static String pathPreProcess(String path, String fileSeparator) {
        if (path.endsWith(fileSeparator)) {
            return path;
        } else {
            return path + fileSeparator;
        }
    }

    /**
     * @param path
     * @return 判断传入的路径最后一个字符是否是分隔符，不是的话，添加分隔符
     */
    public static String hdfsPathPreProcess(String path) {
        return pathPreProcess(path, Path.SEPARATOR);
    }

    /**
     * @param path
     * @return 判断传入的路径最后一个字符是否是分隔符，不是的话，添加分隔符
     */
    public static String pathPreProcess(String path) {
        return pathPreProcess(path, File.separator);
    }

    public static Properties loadProperty(String file) throws Exception {
        Properties props = new Properties();
        InputStream fStream;
        if (file.startsWith("hdfs:")) {
            Path pt = new Path(file);
            FileSystem fs = FileSystem.get(new Configuration());
            fStream = fs.open(pt);
        } else {
            File file2 = new File(file);
            fStream = new FileInputStream(file2);
        }
        props.load(fStream);
        fStream.close();
        return props;
    }

    /**
     * 遍历目录下及子目录下的文件，不返回目录
     *
     * @param directory             ,扫描的目录
     * @param recursive，是否扫描子目录的子目录 ,true,扫描子目录及其子目录，false，则只扫描一层子目录
     * @return , 如果传入的参数dir本身就是文件，则返回该文件本身
     */
    public static List<File> listFiles(File directory, Boolean recursive) {
        List<File> list = new ArrayList<>();

        if (directory.isFile()) {
            list.add(directory);
            return list;
        }

        for (File file : directory.listFiles()) {
            if (file.isFile()) {
                list.add(file);
                continue;
            }
            if (recursive) {
                list.addAll(listFiles(file, true));
            }
        }
        return list;
    }

    public static List<String> listFiles(String directory, Boolean recursive){
        List<String> list = new ArrayList<>();
        for(File file:listFiles(new File(directory), recursive)){
            list.add(file.getAbsolutePath());
        }
        return list;
    }

    /**
     *
     * @param directory
     * @return 只有一个参数，表示只列举该目录下的子文件，不返回目录
     */
    public static List<File> listFiles(File directory){
        return listFiles(directory,false);
    }

    /**
     *
     * @param directory
     * @return 只有一个参数，表示只列举该目录下的子文件，不返回目录
     */
    public static List<String> listFiles(String directory){
        return listFiles(directory,false);
    }

}
