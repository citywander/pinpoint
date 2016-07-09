/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.plugin.jdbc.hana.interceptor;

import java.util.Arrays;
import java.util.Properties;

import com.navercorp.pinpoint.bootstrap.context.DatabaseInfo;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetConstructor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DefaultDatabaseInfo;
import com.navercorp.pinpoint.bootstrap.util.InterceptorUtils;
import com.navercorp.pinpoint.plugin.jdbc.hana.HanaConstants;
import com.navercorp.pinpoint.plugin.jdbc.hana.HanaJdbcUrlParser;

/**
 * @author emeroad
 */
@TargetConstructor({ "com.sap.db.rte.comm.JdbcCommunication", "java.util.Properties", "com.sap.db.jdbc.trace.TraceControl" })
public class HanaConnectionCreateInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final TraceContext traceContext;

    public HanaConnectionCreateInterceptor(TraceContext traceContext) {
        this.traceContext = traceContext;
    }

    //    jdbc:sap://10.58.108.180:30215?reconnect=true&currentschema=JOBDB8
    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args, result, throwable);
        }
        if (args == null || args.length != 3) {
            return;
        }
        
        Properties properties = (Properties)args[1];
        String dburl = properties.getProperty("dburl") + "?" + "currentschema=" + properties.getProperty("currentschema");
        final HanaJdbcUrlParser jdbcUrlParser = new HanaJdbcUrlParser();
        // In case of loadbalance, connectUrl is modified.
        // final String url = getString(args[4]);
        DatabaseInfo databaseInfo = jdbcUrlParser.parse(dburl);

        if (InterceptorUtils.isSuccess(throwable)) {
            // Set only if connection is success.
            if (target instanceof DatabaseInfoAccessor) {
                ((DatabaseInfoAccessor) target)._$PINPOINT$_setDatabaseInfo(databaseInfo);
            }
        }

        final Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            return;
        }

        SpanEventRecorder recorder = trace.currentSpanEventRecorder();
        // We must do this if current transaction is being recorded.
        if (databaseInfo != null) {
            recorder.recordServiceType(databaseInfo.getType());
            recorder.recordEndPoint(databaseInfo.getMultipleHost());
            recorder.recordDestinationId(databaseInfo.getDatabaseId());
        }

    }
    
    private DatabaseInfo createDatabaseInfo(String url, Integer port, String databaseId) {
        if (url.indexOf(':') == -1) {
            url += ":" + port;
        }

        DatabaseInfo databaseInfo = new DefaultDatabaseInfo(HanaConstants.HANA, HanaConstants.HANA_EXECUTE_QUERY, url, url, Arrays.asList(url), databaseId);
        return databaseInfo;

    }

    private String getString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return null;
    }

    private Integer getInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return null;
    }

    @Override
    public void before(Object target, Object[] args) {

    }
}
