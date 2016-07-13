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


import com.navercorp.pinpoint.bootstrap.context.DatabaseInfo;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.annotation.TargetConstructor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor;
import com.navercorp.pinpoint.bootstrap.util.InterceptorUtils;

/**
 * @author emeroad
 */
@TargetConstructor({ "java.sql.Connection", "com.sap.db.jdbc.trace.TraceControl" })
public class HanaTraceConnectionCreateInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final TraceContext traceContext;

    public HanaTraceConnectionCreateInterceptor(TraceContext traceContext) {
        this.traceContext = traceContext;
    }

    //    jdbc:sap://10.58.108.180:30215?reconnect=true&currentschema=JOBDB8
    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args, result, throwable);
        }
        if (args == null || args.length != 2) {
            return;
        }
        DatabaseInfo databaseInfo = (target instanceof DatabaseInfoAccessor) ? ((DatabaseInfoAccessor)args[0])._$PINPOINT$_getDatabaseInfo() : null;

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

    @Override
    public void before(Object target, Object[] args) {

    }
}
