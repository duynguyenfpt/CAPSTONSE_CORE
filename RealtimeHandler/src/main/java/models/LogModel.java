package models;

public class LogModel {
    private int job_id;
    private int request_id;
    private String host;
    private String port;
    private String database_name;
    private String table_name;
    private int step;
    private String request_type;
    private int number_step;
    private String message;
    private String status;

    public int getJob_id() {
        return job_id;
    }

    public void setJob_id(int job_id) {
        this.job_id = job_id;
    }

    public int getRequest_id() {
        return request_id;
    }

    public void setRequest_id(int request_id) {
        this.request_id = request_id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDatabase_name() {
        return database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getRequest_type() {
        return request_type;
    }

    public void setRequest_type(String request_type) {
        this.request_type = request_type;
    }

    public int getNumber_step() {
        return number_step;
    }

    public void setNumber_step(int number_step) {
        this.number_step = number_step;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LogModel(int job_id, int request_id, String host, String port, String database_name, String table_name, int step, String request_type, int number_step, String message, String status) {
        this.job_id = job_id;
        this.request_id = request_id;
        this.host = host;
        this.port = port;
        this.database_name = database_name;
        this.table_name = table_name;
        this.step = step;
        this.request_type = request_type;
        this.number_step = number_step;
        this.message = message;
        this.status = status;
    }
}
