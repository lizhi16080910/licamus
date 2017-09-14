package com;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by lfq on 2017/4/11.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        Path ab = Paths.get("f:\\", "test");
        System.out.println(ab.toAbsolutePath());
        Files.createFile(Paths.get(""));

    }
}
