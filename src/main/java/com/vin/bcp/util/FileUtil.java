package com.vin.bcp.util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * Util class to work with File
 * 
 */
public class FileUtil {
    public static String readFileToString(String file) {
        StringBuilder sb = new StringBuilder();
        try (Scanner scanner = new Scanner(Paths.get(file))) {
            while (scanner.hasNextLine()) {
                sb.append(scanner.nextLine());
                sb.append(" ");
            }
            return sb.toString().trim();
        } catch (IOException e) {
            throw new RuntimeException("Not able to parse content from " + file);
        }
    }

}
