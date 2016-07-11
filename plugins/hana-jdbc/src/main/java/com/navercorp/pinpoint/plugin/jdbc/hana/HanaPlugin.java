/*
 * Copyright 2014 NAVER Corp.
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
package com.navercorp.pinpoint.plugin.jdbc.hana;

import java.security.ProtectionDomain;

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.ExecutionPolicy;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.PreparedStatementBindingMethodFilter;

import static com.navercorp.pinpoint.common.util.VarArgs.va;

/**
 * @author Jongho Moon
 */
public class HanaPlugin implements ProfilerPlugin, TransformTemplateAware {

    private TransformTemplate transformTemplate;

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        HanaConfig config = new HanaConfig(context.getConfig());

        addConnectionTransformer("com.sap.db.jdbc.ConnectionSapDB", config, false);
        addConnectionTransformer("com.sap.db.jdbc.trace.Connection", config, true);
        addDriverTransformer();
        addStatementTransformer();
        addPreparedStatementTransformer(config);
        //addCallableStatementTransformer(config);

        // From MySQL driver 5.1.x, backward compatibility is broken.
        // Driver returns not com.mysql.jdbc.Connection but com.mysql.jdbc.JDBC4Connection which extends com.mysql.jdbc.ConnectionImpl from 5.1.x
        //addJDBC4PreparedStatementTransformer(config);
        //addJDBC4CallableStatementTransformer(config);
    }

    private void addConnectionTransformer(String className, final HanaConfig config, final boolean trace) {
        TransformCallback transformer = new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (!target.isInterceptable()) {
                    return null;
                }

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");
                if(trace){
                    target.addInterceptor("com.navercorp.pinpoint.plugin.jdbc.hana.interceptor.HanaTraceConnectionCreateInterceptor");
                }else{
                    target.addInterceptor("com.navercorp.pinpoint.plugin.jdbc.hana.interceptor.HanaConnectionCreateInterceptor");
                }
                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.ConnectionCloseInterceptor", HanaConstants.HANA_SCOPE);
                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementCreateInterceptor", HanaConstants.HANA_SCOPE);
                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementCreateInterceptor", HanaConstants.HANA_SCOPE);

                if (config.isProfileSetAutoCommit()) {
                    target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionSetAutoCommitInterceptor", HanaConstants.HANA_SCOPE);
                }

                if (config.isProfileCommit()) {
                    target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionCommitInterceptor", HanaConstants.HANA_SCOPE);
                }

                if (config.isProfileRollback()) {
                    target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionRollbackInterceptor", HanaConstants.HANA_SCOPE);
                }

                return target.toBytecode();
            }
        };

        //transformTemplate.transform("com.sap.db.jdbc.trace.Connection", transformer);
 
        transformTemplate.transform(className, transformer);
        //transformTemplate.transform("com.mysql.jdbc.ConnectionImpl", transformer);
    }

    private void addDriverTransformer() {
        transformTemplate.transform("com.sap.db.jdbc.Driver", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.DriverConnectInterceptor", va(new HanaJdbcUrlParser(), false), HanaConstants.HANA_SCOPE, ExecutionPolicy.ALWAYS);

                return target.toBytecode();
            }
        });
    }

    private void addPreparedStatementTransformer(final HanaConfig config) {
        transformTemplate.transform("com.sap.db.jdbc.CallableStatementSapDB", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor");

                int maxBindValueSize = config.getMaxSqlBindValueSize();

                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementExecuteQueryInterceptor", va(maxBindValueSize), HanaConstants.HANA_SCOPE);

                if (config.isTraceSqlBindValue()) {
                    final PreparedStatementBindingMethodFilter excludes = PreparedStatementBindingMethodFilter.excludes("setRowId", "setNClob", "setSQLXML");
                    target.addScopedInterceptor(excludes, "com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementBindVariableInterceptor", HanaConstants.HANA_SCOPE, ExecutionPolicy.BOUNDARY);
                }

                return target.toBytecode();
            }
        });
    }

    private void addCallableStatementTransformer(final HanaConfig config) {
        transformTemplate.transform("com.sap.db.jdbc.trace.CallableStatement", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor");
                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor");

                int maxBindValueSize = config.getMaxSqlBindValueSize();

                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.CallableStatementExecuteQueryInterceptor", va(maxBindValueSize), HanaConstants.HANA_SCOPE);
                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.CallableStatementRegisterOutParameterInterceptor", HanaConstants.HANA_SCOPE);

                if (config.isTraceSqlBindValue()) {
                    final PreparedStatementBindingMethodFilter excludes = PreparedStatementBindingMethodFilter.excludes("setRowId", "setNClob", "setSQLXML");
                    target.addScopedInterceptor(excludes, "com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.CallableStatementBindVariableInterceptor", HanaConstants.HANA_SCOPE, ExecutionPolicy.BOUNDARY);
                }

                return target.toBytecode();
            }
        });
    }

   /* private void addJDBC4PreparedStatementTransformer(final HanaConfig config) {
        transformTemplate.transform("com.mysql.jdbc.JDBC4PreparedStatement", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (config.isTraceSqlBindValue()) {
                    final PreparedStatementBindingMethodFilter includes = PreparedStatementBindingMethodFilter.includes("setRowId", "setNClob", "setSQLXML");
                    target.addScopedInterceptor(includes, "com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementBindVariableInterceptor", HanaConstants.HANA_SCOPE, ExecutionPolicy.BOUNDARY);
                }

                return target.toBytecode();
            }
        });
    }*/

/*    private void addJDBC4CallableStatementTransformer(final HanaConfig config) {
        transformTemplate.transform("com.mysql.jdbc.JDBC4CallableStatement", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (config.isTraceSqlBindValue()) {
                    final PreparedStatementBindingMethodFilter includes = PreparedStatementBindingMethodFilter.includes("setRowId", "setNClob", "setSQLXML");
                    target.addScopedInterceptor(includes, "com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.CallableStatementBindVariableInterceptor", HanaConstants.HANA_SCOPE, ExecutionPolicy.BOUNDARY);
                }

                return target.toBytecode();
            }
        });
    }*/

    private void addStatementTransformer() {
        TransformCallback transformer = new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                if (!target.isInterceptable()) {
                    return null;
                }

                target.addField("com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor");

                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementExecuteQueryInterceptor", HanaConstants.HANA_SCOPE);
                target.addScopedInterceptor("com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementExecuteUpdateInterceptor", HanaConstants.HANA_SCOPE);

                return target.toBytecode();
            }
        };

        transformTemplate.transform("com.sap.db.jdbc.trace.Statement", transformer);
        transformTemplate.transform("com.sap.db.jdbc.StatementSapDB", transformer);

    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
}
