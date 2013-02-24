/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tomcat.jdbc.pool;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;

/**
 * A ProxyConnection object is the bottom most interceptor that wraps an object of type
 * {@link PooledConnection}. The ProxyConnection intercepts three methods:
 * <ul>
 *   <li>{@link java.sql.Connection#close()} - returns the connection to the pool. May be called multiple times.</li>
 *   <li>{@link java.lang.Object#toString()} - returns a custom string for this object</li>
 *   <li>{@link javax.sql.PooledConnection#getConnection()} - returns the underlying connection</li>
 * </ul>
 * By default method comparisons is done on a String reference level, unless the {@link PoolConfiguration#setUseEquals(boolean)} has been called
 * with a <code>true</code> argument.
 * @author Filip Hanik
 */
public class ProxyConnection extends JdbcInterceptor {

    final PooledConnection connection;

    final ConnectionPool pool;

    private volatile boolean closed;
    
    public ConnectionPool getPool() {
        return pool;
    }

    protected ProxyConnection(ConnectionPool parent, PooledConnection con) {
    	super(null, null);
        pool = parent;
        connection = con;
    }

	@Override
	public void initialize(ConnectionPool parent, PooledConnection con) {
		closed = false;

		super.initialize(parent, con);
	}
    
    @Override
    public void cleanup() {
    	super.cleanup();
    	
    	closed = true;
    }

    public boolean isWrapperFor(Class<?> iface) {
        if (iface == XAConnection.class && connection.getXAConnection()!=null) {
            return true;
        } else {
            return (iface.isInstance(connection.getConnection()));
        }
    }


    public Object unwrap(Class<?> iface) throws SQLException {
        if (iface == PooledConnection.class) {
            return connection;
        }else if (iface == XAConnection.class) {
            return connection.getXAConnection();
        } else if (isWrapperFor(iface)) {
            return connection.getConnection();
        } else {
            throw new SQLException("Not a wrapper of "+iface.getName());
        }
    }

    @Override
    public Object invokeMethod(Connection proxy, Method method, Object[] args) throws Throwable {
        if (compare(ISCLOSED_VAL,method)) {
            return Boolean.valueOf(isClosed());
        }
        if (compare(CLOSE_VAL,method)) {
            if (closed) {
            	return null; //noop for already closed.
            }
            closed = true;
            pool.returnConnection(this.connection);
            return null;
        } else if (compare(TOSTRING_VAL,method)) {
            return this.toString();
        } else if (compare(GETCONNECTION_VAL,method) && connection!=null) {
            return connection.getConnection();
        } else if (method.getDeclaringClass().equals(XAConnection.class)) {
            try {
                return method.invoke(connection.getXAConnection(),args);
            }catch (Throwable t) {
                if (t instanceof InvocationTargetException) {
                    throw t.getCause() != null ? t.getCause() : t;
                } else {
                    throw t;
                }
            }
        }
        if (isClosed()) throw new SQLException("Connection has already been closed.");
        if (compare(UNWRAP_VAL,method)) {
            return unwrap((Class<?>)args[0]);
        } else if (compare(ISWRAPPERFOR_VAL,method)) {
            return Boolean.valueOf(this.isWrapperFor((Class<?>)args[0]));
        }
        try {
            PooledConnection poolc = connection;
            if (poolc!=null) {
                return method.invoke(poolc.getConnection(),args);
            } else {
                throw new SQLException("Connection has already been closed.");
            }
        }catch (Throwable t) {
            if (t instanceof InvocationTargetException) {
                throw t.getCause() != null ? t.getCause() : t;
            } else {
                throw t;
            }
        }
    }

    public boolean isClosed() {
        return closed || connection.isDiscarded();
    }

    public PooledConnection getDelegateConnection() {
        return connection;
    }

    public ConnectionPool getParentPool() {
        return pool;
    }

    @Override
    public String toString() {
        return "ProxyConnection["+(connection!=null?connection.toString():"null")+"]";
    }

	@Override
	public PooledConnection getPooledConnection() {
		return connection;
	}


}
