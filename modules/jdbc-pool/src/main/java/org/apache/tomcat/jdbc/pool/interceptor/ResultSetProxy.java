package org.apache.tomcat.jdbc.pool.interceptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Because we call result set methods for each column of each row, we don't want the overhead of a dynamic proxy
 *
 */
public class ResultSetProxy extends DelegatingResultSet {

    final Statement statement;

    public ResultSetProxy(ResultSet inner, Statement statement) {
        super(inner);
        this.statement = statement;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

}
