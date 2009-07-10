package org.apache.tomcat.jdbc.test;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.test.driver.Driver;

public class TestConcurrency extends DefaultTestCase {

    public static final  boolean debug = Boolean.getBoolean("jdbc.debug");
    
    protected volatile DataSource ds = null;
    
    public TestConcurrency(String name) {
        super(name);
    }
    
    @Override
    public void setUp() {
        // TODO Auto-generated method stub
        ds = createDefaultDataSource();
        ds.getPoolProperties().setDriverClassName(Driver.class.getName());
        ds.getPoolProperties().setUrl(Driver.url);
        ds.getPoolProperties().setInitialSize(0);
        ds.getPoolProperties().setMaxIdle(0);
        ds.getPoolProperties().setMinIdle(0);
        ds.getPoolProperties().setMaxActive(10);
        ds.getPoolProperties().setRemoveAbandoned(true);
        ds.getPoolProperties().setLogAbandoned(true);
        ds.getPoolProperties().setTestWhileIdle(true);
        ds.getPoolProperties().setMinEvictableIdleTimeMillis(750);
        ds.getPoolProperties().setTimeBetweenEvictionRunsMillis(25);
    }

    @Override
    protected void tearDown() throws Exception {
        Driver.reset();
        super.tearDown();
    }
    
    public void testSimple() throws Exception {
        ds.getConnection().close();
        final int iter = 1000 * 10;
        final AtomicInteger loopcount = new AtomicInteger(0);
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while (loopcount.incrementAndGet() < iter) {
                        Connection con = ds.getConnection();
                        Thread.sleep(10);
                        con.close();
                    }
                }catch (Exception x) {
                    loopcount.set(iter); //stops the test
                    x.printStackTrace();
                }
            }
        };
        Thread[] threads = new Thread[20];
        for (int i=0; i<threads.length; i++) {
            threads[i] = new Thread(run);
        }
        for (int i=0; i<threads.length; i++) {
            threads[i].start();
        }
        try {
            while (loopcount.get()<iter) {
                assertEquals("Size comparison:",10, ds.getPool().getSize());
                if (debug) {
                    System.out.println("Size: "+ds.getPool().getSize());
                    System.out.println("Used: "+ds.getPool().getActive());
                    System.out.println("Idle: "+ds.getPool().getIdle());
                    System.out.println("Wait: "+ds.getPool().getWaitCount());
                }
                Thread.sleep(250);
            }
        }catch (Exception x) {
            loopcount.set(iter); //stops the test
            x.printStackTrace();
        }
        for (int i=0; i<threads.length; i++) {
            threads[i].join();
        }
        assertEquals("Size comparison:",10, ds.getPool().getSize());
        assertEquals("Idle comparison:",10, ds.getPool().getIdle());
        assertEquals("Used comparison:",0, ds.getPool().getActive());
        assertEquals("Connect count",10,Driver.connectCount.get());
            
    }
    
    public void testBrutal() throws Exception {
        ds.getPoolProperties().setRemoveAbandoned(false);
        ds.getPoolProperties().setMinEvictableIdleTimeMillis(-1);
        ds.getPoolProperties().setTimeBetweenEvictionRunsMillis(-1);
        ds.getConnection().close();
        final int iter = 1000 * 10;
        final AtomicInteger loopcount = new AtomicInteger(0);
        final Runnable run = new Runnable() {
            public void run() {
                try {
                    while (loopcount.incrementAndGet() < iter) {
                        Connection con = ds.getConnection();
                        Thread.sleep(10);
                        con.close();
                    }
                }catch (Exception x) {
                    loopcount.set(iter); //stops the test
                    x.printStackTrace();
                }
            }
        };
        Thread[] threads = new Thread[20];
        for (int i=0; i<threads.length; i++) {
            threads[i] = new Thread(run);
        }
        for (int i=0; i<threads.length; i++) {
            threads[i].start();
        }
        try {
            while (loopcount.get()<iter) {
                //assertEquals("Size comparison:",10, ds.getPool().getSize());
                ds.getPool().testAllIdle();
                ds.getPool().checkAbandoned();
                ds.getPool().checkIdle();
            }
        }catch (Exception x) {
            loopcount.set(iter); //stops the test
            x.printStackTrace();
        }
        for (int i=0; i<threads.length; i++) {
            threads[i].join();
        }
        System.out.println("Connect count:"+Driver.connectCount.get());
        System.out.println("DisConnect count:"+Driver.disconnectCount.get());
        assertEquals("Size comparison:",10, ds.getPool().getSize());
        assertEquals("Idle comparison:",10, ds.getPool().getIdle());
        assertEquals("Used comparison:",0, ds.getPool().getActive());
        assertEquals("Connect count",10,Driver.connectCount.get());
            
    }

    
}
