package com.netflix.conductor.server;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class FaultInjectionWithProbability implements MethodInterceptor {

    DynamicFailureProbability failureProbability;

    public FaultInjectionWithProbability(DynamicFailureProbability failureProbability) {
        this.failureProbability = failureProbability;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        // TODO set method name in DynamicFailureProbability for explicitly failing a particular method instead of all.
        if (
                (invocation.getMethod().getName().equals(failureProbability.getMethodName()) || "*".equals(failureProbability.getMethodName())) &&
                (failureProbability.get() > 0 && (Math.random() <= failureProbability.get()))
        ) {
            throw new IllegalStateException(
                    invocation.getMethod().getName() + " failed with provided probability: " + failureProbability.get());
        } else {
            return invocation.proceed();
        }
    }

    public class DynamicFailureProbability {
        private String className;
        private String methodName;
        private double failureProbability = 0;

        public double get() {
            return failureProbability;
        }

        public void setFailureProbability(double failureProbability) {
            this.failureProbability = failureProbability;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }
    }
}
