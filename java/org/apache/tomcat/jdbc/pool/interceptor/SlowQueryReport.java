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
package org.apache.tomcat.jdbc.pool.interceptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.JdbcInterceptor;
import org.apache.tomcat.jdbc.pool.PooledConnection;

/**
 * Slow query report interceptor. Tracks timing of query executions.
 * @author Filip Hanik
 * @version 1.0
 */
public class SlowQueryReport extends AbstractCreateStatementInterceptor implements ConnectionPool.CloseListener {
    //logger
    protected static Log log = LogFactory.getLog(SlowQueryReport.class);
    /**
     * the constructors that are used to create statement proxies 
     */
    protected static final Constructor[] constructors = new Constructor[AbstractCreateStatementInterceptor.statements.length];
    /**
     * we will be keeping track of query stats on a per pool basis, do we want this, or global?
     */
    protected static IdentityHashMap<ConnectionPool,HashMap<String,QueryStats>> perPoolStats = 
        new IdentityHashMap<ConnectionPool,HashMap<String,QueryStats>>();
    /**
     * the queries that are used for this interceptor.
     */
    protected HashMap<String,QueryStats> queries = null;
    /**
     * The threshold in milliseconds. If the query is faster than this, we don't measure it
     */
    protected long threshold = 100; //don't report queries less than this
    /**
     * Maximum number of queries we will be storing
     */
    protected int  maxQueries= 1000; //don't store more than this amount of queries
    
    /**
     * The pool that is associated with this interceptor so that we can clean up
     */
    private ConnectionPool pool = null;

    /**
     * Returns the query stats for a given pool
     * @param pool - the pool we want to retrieve stats for
     * @return a hash map containing statistics for 0 to maxQueries 
     */
    public static HashMap<String,QueryStats> getPoolStats(ConnectionPool pool) {
        return perPoolStats.get(pool);
    }
    
    /**
     * Creates a slow query report interceptor
     */
    public SlowQueryReport() {
        super();
    }

    /**
     * returns the query measure threshold.
     * This value is in milliseconds. If the query is faster than this threshold than it wont be accounted for
     * @return
     */
    public long getThreshold() {
        return threshold;
    }

    /**
     * Sets the query measurement threshold. The value is in milliseconds.
     * If the query goes faster than this threshold it will not be recorded.
     * @param threshold set to -1 to record every query. Value is in milliseconds.
     */
    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    /**
     * invoked when the connection receives the close request
     * Not used for now.
     */
    @Override
    public void closeInvoked() {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * Creates a constructor for a proxy class, if one doesn't already exist
     * @param idx - the index of the constructor
     * @param clazz - the interface that the proxy will implement
     * @return - returns a constructor used to create new instances
     * @throws NoSuchMethodException
     */
    protected Constructor getConstructor(int idx, Class clazz) 
        throws NoSuchMethodException {
        if (constructors[idx]==null) {
            Class proxyClass = Proxy.getProxyClass(SlowQueryReport.class.getClassLoader(), new Class[] {clazz});
            constructors[idx] = proxyClass.getConstructor(new Class[] { InvocationHandler.class });
        }
        return constructors[idx];
    }

    /**
     * Creates a statement interceptor to monitor query response times
     */
    @Override
    public Object createStatement(Object proxy, Method method, Object[] args, Object statement) {
        try {
            Object result = null;
            String name = method.getName();
            String sql = null;
            Constructor constructor = null;
            if (compare(statements[0],name)) {
                //createStatement
                constructor = getConstructor(0,Statement.class);
            }else if (compare(statements[1],name)) {
                //prepareStatement
                sql = (String)args[0];
                constructor = getConstructor(1,PreparedStatement.class);
            }else if (compare(statements[2],name)) {
                //prepareCall
                sql = (String)args[0];
                constructor = getConstructor(2,CallableStatement.class);
            }else {
                //do nothing, might be a future unsupported method
                //so we better bail out and let the system continue
                return statement;
            }
            result = constructor.newInstance(new Object[] { new StatementProxy(statement,sql) });
            return result;
        }catch (Exception x) {
            log.warn("Unable to create statement proxy for slow query report.",x);
        }
        return statement;
    }

    /**
     * {@inheritDoc}
     */
    public void reset(ConnectionPool parent, PooledConnection con) {
        //see if we already created a map for this pool
        queries = SlowQueryReport.perPoolStats.get(parent);
        if (queries==null) {
            //create the map to hold our stats
            //however TODO we need to improve the eviction
            //selection
            queries = new LinkedHashMap<String,QueryStats>() {
                @Override
                protected boolean removeEldestEntry(Entry<String, QueryStats> eldest) {
                    return size()>maxQueries;
                }

            };
            perPoolStats.put(parent, queries);
            //add ourselves to the pool listener, so we can cleanup
            parent.addCloseListener(this);
        }
        this.pool = parent;
    }

    
    public void poolClosed(ConnectionPool pool) {
        //clean up after ourselves.
        perPoolStats.remove(pool);
    }
    

    
    /**
     * 
     * @author fhanik
     *
     */
    public static class QueryStats {
        private final String query;
        private int nrOfInvocations;
        private long maxInvocationTime = Long.MIN_VALUE;
        private long maxInvocationDate;
        private long minInvocationTime = Long.MAX_VALUE;
        private long minInvocationDate;
        private long totalInvocationTime;
        
        public String toString() {
            StringBuffer buf = new StringBuffer("QueryStats[query:");
            buf.append(query);
            buf.append(", nrOfInvocations:");
            buf.append(nrOfInvocations);
            buf.append(", maxInvocationTime:");
            buf.append(maxInvocationTime);
            buf.append(", maxInvocationDate:");
            buf.append(new java.util.Date(maxInvocationDate).toGMTString());
            buf.append(", minInvocationTime:");
            buf.append(minInvocationTime);
            buf.append(", minInvocationDate:");
            buf.append(new java.util.Date(minInvocationDate).toGMTString());
            buf.append(", totalInvocationTime:");
            buf.append(totalInvocationTime);
            buf.append(", averageInvocationTime:");
            buf.append((float)totalInvocationTime / (float)nrOfInvocations);
            buf.append("]");
            return buf.toString();
        }
        
        public QueryStats(String query) {
            this.query = query;
        }
        
        public void add(long invocationTime, long now) {
            //not thread safe, but don't sacrifice performance for this kind of stuff
            maxInvocationTime = Math.max(invocationTime, maxInvocationTime);
            if (maxInvocationTime == invocationTime) {
                maxInvocationDate = now;
            }
            minInvocationTime = Math.min(invocationTime, minInvocationTime);
            if (minInvocationTime==invocationTime) {
                minInvocationDate = now;
            }
            nrOfInvocations++;
            totalInvocationTime+=invocationTime;
        }
        
        public String getQuery() {
            return query;
        }

        public int getNrOfInvocations() {
            return nrOfInvocations;
        }

        public long getMaxInvocationTime() {
            return maxInvocationTime;
        }

        public long getMaxInvocationDate() {
            return maxInvocationDate;
        }

        public long getMinInvocationTime() {
            return minInvocationTime;
        }

        public long getMinInvocationDate() {
            return minInvocationDate;
        }

        public long getTotalInvocationTime() {
            return totalInvocationTime;
        }

        public int hashCode() {
            return query.hashCode();
        }
        
        public boolean equals(Object other) {
            if (other instanceof QueryStats) {
                QueryStats qs = (QueryStats)other;
                return qs.query.equals(this.query);
            } 
            return false;
        }
    }
    
    /**
     * Class to measure query execute time
     * @author fhanik
     *
     */
    protected class StatementProxy implements InvocationHandler {
        protected boolean closed = false;
        protected Object delegate;
        protected final String query;
        public StatementProxy(Object parent, String query) {
            this.delegate = parent;
            this.query = query;
        }
        
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            //get the name of the method for comparison
            final String name = method.getName();
            //was close invoked?
            boolean close = compare(JdbcInterceptor.CLOSE_VAL,name);
            //allow close to be called multiple times
            if (close && closed) return null; 
            //are we calling isClosed?
            if (compare(JdbcInterceptor.ISCLOSED_VAL,name)) return closed;
            //if we are calling anything else, bail out
            if (closed) throw new SQLException("Statement closed.");
            boolean process = false;
            //check to see if we are about to execute a query
            process = process(executes, method, process);
            //if we are executing, get the current time
            long start = (process)?System.currentTimeMillis():0;
            //execute the query
            Object result =  method.invoke(delegate,args);
            //measure the time
            long delta = (process)?(System.currentTimeMillis()-start):Long.MIN_VALUE;
            //see if we meet the requirements to measure
            if (delta>threshold) {
                //extract the query string
                String sql = (query==null && args!=null &&  args.length>0)?(String)args[0]:query;
                //if we do batch execution, then we name the query 'batch'
                if (sql==null && compare(executes[3],name)) {
                    sql = "batch";
                }
                //if we have a query, record the stats
                if (sql!=null) {
                    QueryStats qs = SlowQueryReport.this.queries.get(sql);
                    if (qs == null) {
                        qs = new QueryStats(sql);
                        SlowQueryReport.this.queries.put((String)sql,qs);
                    }
                    qs.add(delta,start);
                }
            }
            //perform close cleanup
            if (close) {
                closed=true;
                delegate = null;
                queries = null;
            }
            return result;
        }
    }
}
