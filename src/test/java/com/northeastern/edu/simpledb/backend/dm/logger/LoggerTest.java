package com.northeastern.edu.simpledb.backend.dm.logger;

import com.northeastern.edu.simpledb.backend.dm.logger.Logger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LoggerTest {

    private static final short OF_CHECKSUM = 4;
    private static Logger logger;

    private static final String LOG_NAME = "bin";

    @BeforeAll
    static void setup() {
        logger = Logger.create(LOG_NAME);
    }

    @AfterAll
    static void cleanTestEnv() {
        try {
            Files.deleteIfExists(Path.of(LOG_NAME + Logger.LOG_SUFFIX));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            logger.close();
        }
    }

    @Test
    @Order(1)
    void testInit_expectedFCPositionEqualsToOffsetCheckSumAfterInit() {
        try {
            Method initMethod = logger.getClass().getDeclaredMethod("init");
            initMethod.setAccessible(true);
            initMethod.invoke(logger);
            Assertions.assertEquals(OF_CHECKSUM, logger.getFc().position());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Order(2)
    void testLogOnce_expectedFCPositionEqualsToOffsetCheckSumAfterLog() {
        String sql = "select * from test";
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        logger.log(bytes);
        try {
            Assertions.assertEquals(OF_CHECKSUM, logger.getFc().position());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Order(3)
    void testLogOnce_expectedSameSQL() {
        String expected = "select * from test";
        logger = Logger.open(LOG_NAME);
        byte[] log = logger.next();
        assert log != null;
        String actual = new String(log);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    @Order(4)
    void testLogMultipleTimes_expectedSameSQL() {
        String sql1 = "select * from test";
        logger.log(sql1.getBytes(StandardCharsets.UTF_8));

        String sql2 = "select * from test where a = 1";
        logger.log(sql2.getBytes(StandardCharsets.UTF_8));

        String sql3 = "update from test set b = 3 where a = 1";
        logger.log(sql3.getBytes(StandardCharsets.UTF_8));

        logger = Logger.open(LOG_NAME);

        logger.next();

        byte[] log1 = logger.next();
        assert log1 != null;
        String res1 = new String(log1);
        Assertions.assertEquals(sql1, res1);

        byte[] log2 = logger.next();
        assert log2 != null;
        String res2 = new String(log2);
        Assertions.assertEquals(sql2, res2);

        byte[] log3 = logger.next();
        assert log3 != null;
        String res3 = new String(log3);
        Assertions.assertEquals(sql3, res3);
    }

}
