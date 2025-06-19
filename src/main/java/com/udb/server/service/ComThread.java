package com.udb.server.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.udb.server.bodies.Result;

/**
 * This class extends the Thread class and is used to execute SQL statements in
 * a separate thread.
 * It provides methods for managing the execution of SQL statements, including
 * starting, ending, and committing/rolling back transactions.
 * It also provides methods for managing the execution of SQL statements in a
 * transaction.
 * It uses HikariCP for connection pooling and FastJSON2 for JSON processing.
 */
public class ComThread extends Thread {
    protected int status;
    protected String sessionId;
    protected Object param;

    public String getType() {
        return "";
    }

    public int getStatus() {
        return status;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getLable() {
        return "";
    }

    protected Connection conn;
    protected Statement stmt;

    public Connection getConn() {
        return conn;
    }

    public Statement getStmt() {
        return stmt;
    }

    protected BlockingQueue<Map<String, Object>> results;

    /**
     * Get the results.
     * 
     * @return
     */
    public BlockingQueue<Map<String, Object>> getResults() {
        return results;
    }

    protected String message;

    /**
     * Get the error message.
     * 
     * @return
     */
    public String getMessage() {
        return message;
    }

    public Date startTime;
    public Date endTime;

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    protected boolean isCommitOrRollback;

    public boolean isCommitOrRollback() {
        return isCommitOrRollback;
    }

    protected boolean isTransaction;

    public boolean isTransaction() {
        return isTransaction;
    }

    public void commit() throws SQLException {
        isCommitOrRollback = true;
        conn.commit();
    }

    public void rollback() throws SQLException {
        isCommitOrRollback = true;
        conn.rollback();
    }

    protected double progress;
    public double getProgress() {
        return progress;
    }

    public Result end() {
        // Forcefully end the connection and close the resources
        try {
            if (conn != null) {
                if (isTransaction && !isCommitOrRollback) {

                    status = 850;
                    message = "Transaction has been rollback or commited";
                    return new Result(850).message("Transaction has been rollback or commited");
                }
                conn.close();
            }
            this.interrupt();
            status = 200;
            return Result.success().message("Task has been terminated");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            status = 500;
            message = e.getMessage();
            return Result.error(e.getMessage());
        }
    }
}
