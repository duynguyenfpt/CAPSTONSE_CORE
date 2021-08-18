package models;

import java.util.ArrayList;

public class MergeRequest {
    private String merge_table_name;
    private ArrayList<TableModel> list_tables;
    private ArrayList<MappingModel> list_mapping;

    public MergeRequest(String merge_table_name, ArrayList<TableModel> list_tables, ArrayList<MappingModel> list_mapping) {
        this.merge_table_name = merge_table_name;
        this.list_tables = list_tables;
        this.list_mapping = list_mapping;
    }

    public MergeRequest() {
    }

    public String getMerge_table_name() {
        return merge_table_name;
    }

    public void setMerge_table_name(String merge_table_name) {
        this.merge_table_name = merge_table_name;
    }

    public ArrayList<TableModel> getList_tables() {
        return list_tables;
    }

    public void setList_tables(ArrayList<TableModel> list_tables) {
        this.list_tables = list_tables;
    }

    public ArrayList<MappingModel> getList_mapping() {
        return list_mapping;
    }

    public void setList_mapping(ArrayList<MappingModel> list_mapping) {
        this.list_mapping = list_mapping;
    }
}
