/* Licensed to the Apache Software Foundation (ASF) under one or more
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.PoolProperties.InterceptorProperty;
import org.apache.tomcat.jdbc.pool.PooledConnection;

public class StatementCache extends StatementDecoratorInterceptor {
    protected static final String[] ALL_TYPES = new String[] {PREPARE_STATEMENT,PREPARE_CALL};
    protected static final String[] CALLABLE_TYPE = new String[] {PREPARE_CALL};
    protected static final String[] PREPARED_TYPE = new String[] {PREPARE_STATEMENT};
    protected static final String[] NO_TYPE = new String[] {};

    /*begin properties for the statement cache*/
    private boolean cachePrepared = true;
    private boolean cacheCallable = false;
    private PooledConnection pcon;
    private String[] types;
    private LruCache statements;
    private int maxCacheSize = 50;

    static class LruCache extends LinkedHashMap<String, CachedStatement> {
        private static final long serialVersionUID = 1L;

        final int maxCacheSize;

        public LruCache(int maxCacheSize) {
            super(maxCacheSize, 0.75f, true);
            this.maxCacheSize = maxCacheSize;
        }

        @Override
        protected boolean removeEldestEntry(Entry<String, CachedStatement> eldest) {
            return size() > maxCacheSize;
        }
    }

    public boolean isCachePrepared() {
        return cachePrepared;
    }

    public boolean isCacheCallable() {
        return cacheCallable;
    }

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public String[] getTypes() {
        return types;
    }

    public int getCacheSize() {
        if (statements == null) {
            return 0;
        }
        return statements.size();
    }

    @Override
    public void setProperties(Map<String, InterceptorProperty> properties) {
        super.setProperties(properties);
        InterceptorProperty p = properties.get("prepared");
        if (p!=null) cachePrepared = p.getValueAsBoolean(cachePrepared);
        p = properties.get("callable");
        if (p!=null) cacheCallable = p.getValueAsBoolean(cacheCallable);
        p = properties.get("max");
        if (p!=null) maxCacheSize = p.getValueAsInt(maxCacheSize);
        if (cachePrepared && cacheCallable) {
            this.types = ALL_TYPES;
        } else if (cachePrepared) {
            this.types = PREPARED_TYPE;
        } else if (cacheCallable) {
            this.types = CALLABLE_TYPE;
        } else {
            this.types = NO_TYPE;
        }

    }
    /*end properties for the statement cache*/

    /*begin the actual statement cache*/
    @Override
    public void reset(ConnectionPool parent, PooledConnection con) {
        super.reset(parent, con);
        if (parent==null) {
            this.pcon = null;
        } else {
            this.pcon = con;
            this.statements = new LruCache(maxCacheSize);
        }
    }

    @Override
    public void disconnected(ConnectionPool parent, PooledConnection con, boolean finalizing) {
        assert con == pcon;

        if (statements!=null) {
            for (Map.Entry<String, CachedStatement> p : statements.entrySet()) {
                closeStatement(p.getValue());
            }
            statements.clear();
        }

        super.disconnected(parent, con, finalizing);
    }

    public void closeStatement(CachedStatement st) {
        if (st==null) return;
        st.forceClose();
    }

    @Override
    protected Object createDecorator(Object proxy, Method method, Object[] args,
                                     Object statement, Constructor<?> constructor, String sql)
    throws InstantiationException, IllegalAccessException, InvocationTargetException {
        boolean process = process(this.types, method, false);
        if (process) {
            Object result = null;
            CachedStatement statementProxy = new CachedStatement((Statement)statement,sql);
            result = constructor.newInstance(new Object[] { statementProxy });
            statementProxy.setActualProxy(result);
            statementProxy.setConnection(proxy);
            statementProxy.setConstructor(constructor);
            return result;
        } else {
            return super.createDecorator(proxy, method, args, statement, constructor, sql);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        boolean process = process(this.types, method, false);
        if (process && args.length>0 && args[0] instanceof String) {
            String sql = (String) args[0];
            CachedStatement statement = removeFromCache(sql);
            if (statement!=null) {
                return statement.getActualProxy();
            } else {
                return super.invoke(proxy, method, args);
            }
        } else {
            return super.invoke(proxy,method,args);
        }
    }


    public CachedStatement removeFromCache(String sql) {
        return statements.remove(sql);
    }

    public boolean cacheStatement(CachedStatement proxy) {
        if (proxy.getSql()==null) {
            return false;
        } else if (statements.containsKey(proxy.getSql())) {
            return false;
        } else {
            //cache the statement
            statements.put(proxy.getSql(), proxy);
            return true;
        }
    }

    public boolean removeStatement(CachedStatement proxy) {
        if (statements.remove(proxy.getSql()) != null) {
            return true;
        } else {
            return false;
        }
    }
    /*end the actual statement cache*/


    protected class CachedStatement extends StatementDecoratorInterceptor.StatementProxy<Statement> {
        boolean cached = false;
        boolean broken = false;

        public CachedStatement(Statement parent, String sql) {
            super(parent, sql);
        }

        @Override
        public void closeInvoked() {
            //should we cache it
            boolean shouldClose = true;
            if (!broken) {
                //cache a proxy so that we don't reuse the facade
                CachedStatement proxy = new CachedStatement(getDelegate(),getSql());
                try {
                    //create a new facade
                    Object actualProxy = getConstructor().newInstance(new Object[] { proxy });
                    proxy.setActualProxy(actualProxy);
                    proxy.setConnection(getConnection());
                    proxy.setConstructor(getConstructor());
                    if (cacheStatement(proxy)) {
                        proxy.cached = true;
                        shouldClose = false;
                    }
                } catch (Exception x) {
                    removeStatement(proxy);
                }
            }
            closed = true;
            delegate = null;
            if (shouldClose) {
                super.closeInvoked();
            }

        }

        public void forceClose() {
            removeStatement(this);
            super.closeInvoked();
        }

        @Override
        protected void exceptionReported(Throwable t) {
            broken |= isPermanentFailure(t);
        }

        boolean isPermanentFailure(Throwable t) {
            if (t instanceof SQLException) {
                String sqlState = ((SQLException) t).getSQLState();

                // Postgres error when the underlying schema changes
                if ("0A000".equals(sqlState)) {
                    String message = ((SQLException) t).getMessage();
                    if (message.contains("cached plan must not change result type")) {
                        return true;
                    }
                }
            }

            Throwable cause = t.getCause();
            if (cause == null) {
                return false;
            }

            return isPermanentFailure(cause);
        }

    }

}


