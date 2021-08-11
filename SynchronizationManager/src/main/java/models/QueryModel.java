package models;

public class QueryModel {
    private int requestId;
    private int jobId;
    private String query;
    private int etlID;

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public QueryModel() {
    }

    public int getEtlID() {
        return etlID;
    }

    public void setEtlID(int etlID) {
        this.etlID = etlID;
    }

    public QueryModel(int requestId, int jobId, String query, int etlID) {
        this.requestId = requestId;
        this.jobId = jobId;
        this.query = query;
        this.etlID = etlID;
    }

    public QueryModel(int requestId, int jobId, String query) {
        this.requestId = requestId;
        this.jobId = jobId;
        this.query = query;
    }
}
