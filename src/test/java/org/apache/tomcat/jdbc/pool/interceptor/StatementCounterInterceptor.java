/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.tomcat.jdbc.pool.interceptor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tomcat.jdbc.pool.JdbcInterceptor;
import org.apache.tomcat.jdbc.pool.PoolProperties.InterceptorProperties;

/**
 * Interceptor that counts opened Statements. Is used by tests.
 */
public class StatementCounterInterceptor extends StatementDecoratorInterceptor {

    private final AtomicInteger countOpen = new AtomicInteger();
    private final AtomicInteger countClosed = new AtomicInteger();

    public StatementCounterInterceptor(JdbcInterceptor next, InterceptorProperties properties) {
    	super(next, properties);
    }
    
    public int getActiveCount() {
        return countOpen.get() - countClosed.get();
    }

    @Override
    protected Object createDecorator(Connection proxy, Method method, Object[] args,
            Statement statement, Constructor<? extends Statement> constructor, String sql) throws InstantiationException, IllegalAccessException,
            InvocationTargetException {
        Statement result;
        StatementProxy statementProxy = new StatementProxy(
                (Statement) statement, sql, constructor);
        result = constructor.newInstance(new Object[] { statementProxy });
        statementProxy.setActualProxy(result);
        statementProxy.setConnection(proxy);
        countOpen.incrementAndGet();
        return result;
    }

    private class StatementProxy extends
            StatementDecoratorInterceptor.StatementProxy<Statement> {
        public StatementProxy(Statement delegate, String sql, Constructor<? extends Statement> constructor) {
            super(delegate, sql, constructor);
        }

        @Override
        public void closeInvoked() {
            countClosed.incrementAndGet();
            super.closeInvoked();
        }
    }
}
