package com.fastweb.cdnlog.bigdata.util;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.util.List;

/**
 * Created by lfq on 2017/4/14.
 */
public class ExcelFileUtil {

    /**
     * @param path
     * @param list
     * @throws Exception
     * @throws
     * @Title: toExcel
     * @Description: 以逗号为分隔符
     */
    public static void listToExcel2003(String path, String sheetName, List<String> list)
            throws Exception {
        Workbook wb = new HSSFWorkbook(); // 创建一个excel
        // 声明一个单子并命名
        Sheet sheet = wb.createSheet(sheetName);// excel的一个sheet
        // 给单子名称一个长度
        sheet.setDefaultColumnWidth((short) 15);
        // 生成一个样式
        CellStyle style = wb.createCellStyle(); // excel 的 style

        // 创建第一行（也可以称为表头）
        for (int i = 0; i < list.size(); i++) {
            String[] temp = list.get(i).split(",");
            Row row = sheet.createRow(i);
            for (int j = 0; j < temp.length; j++) {
                Cell cell = row.createCell(j);
                cell.setCellStyle(style);
                cell.setCellValue(temp[j]);
            }
        }
        FileOutputStream out = new FileOutputStream(path);
        wb.write(out);
        out.close();
    }

    /**
     * @param path
     * @param list
     * @throws Exception
     * @throws
     * @Title: toExcel
     * @Description: 以逗号为分隔符
     */
    public static void listToExcel2007(String path, String sheetName, List<String> list)
            throws Exception {
        Workbook wb = new XSSFWorkbook(); // 创建一个excel
        // 声明一个单子并命名
        Sheet sheet = wb.createSheet(sheetName);// excel的一个sheet
        // 给单子名称一个长度
        sheet.setDefaultColumnWidth((short) 15);
        // 生成一个样式
        CellStyle style = wb.createCellStyle(); // excel 的 style

        // 创建第一行（也可以称为表头）
        for (int i = 0; i < list.size(); i++) {
            String[] temp = list.get(i).split(",");
            Row row = sheet.createRow(i);
            for (int j = 0; j < temp.length; j++) {
                Cell cell = row.createCell(j);
                cell.setCellStyle(style);
                cell.setCellValue(temp[j]);
            }
        }
        FileOutputStream out = new FileOutputStream(path);
        wb.write(out);
        out.close();
    }

}
