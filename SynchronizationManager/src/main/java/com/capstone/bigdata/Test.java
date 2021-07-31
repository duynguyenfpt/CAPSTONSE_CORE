package com.capstone.bigdata;

import utils.sqlUtils;

import java.sql.Connection;

public class Test {
    public static void main(String[] args) {
        String createTable = "" +
                "CREATE TABLE IF NOT EXISTS `cdc_detail` (\n" +
                "  `id` int NOT NULL AUTO_INCREMENT,\n" +
                "  `database_url` varchar(45) DEFAULT NULL,\n" +
                "  `database_port` varchar(6) DEFAULT NULL,\n" +
                "  `database_name` varchar(20) DEFAULT NULL,\n" +
                "  `table_name` varchar(20) DEFAULT NULL,\n" +
                "  `schema` varchar(200) DEFAULT NULL,\n" +
                "  `value` varchar(200) DEFAULT NULL,\n" +
                "  `operation` int DEFAULT NULL,\n" +
                "  `created` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ");";
        System.out.println(createTable);
        String prefix = "cdc_4912929_";
        String createDatabase = String.format("create database if not exists %s_cdc", prefix);
        System.out.println(createDatabase);
    }
}
