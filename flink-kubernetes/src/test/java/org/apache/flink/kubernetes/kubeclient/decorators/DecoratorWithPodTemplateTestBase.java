/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesPodTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

/**
 * Test base of merging and overwriting Kubernetes fields from {@link KubernetesConfigOptions} and
 * pod template for the {@link InitJobManagerDecorator} and {@link InitTaskManagerDecorator}.
 */
public abstract class DecoratorWithPodTemplateTestBase extends KubernetesPodTestBase {

    private static final String IMAGE = "test-image:v1";
    private static final String IMAGE_PULL_POLICY = "IfNotPresent";
    private static final List<String> IMAGE_PULL_SECRETS = Arrays.asList("s1", "s2", "s3");
    protected static final Map<String, String> ANNOTATIONS =
            new HashMap<String, String>() {
                private static final long serialVersionUID = 0L;

                {
                    put("a1", "v1");
                    put("a2", "v2");
                }
            };
    protected static final String TOLERATION_STRING =
            "key:key1,operator:Equal,value:value1,effect:NoSchedule";
    private static final String TESTING_SERVICE_ACCOUNT = "testing-service-account";

    protected static final double RESOURCE_CPU = 1.5;
    protected static final int RESOURCE_MEMORY = 1456;

    protected FlinkPod resultPod;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, IMAGE);
        this.flinkConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                KubernetesConfigOptions.ImagePullPolicy.valueOf(IMAGE_PULL_POLICY));
        this.flinkConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, IMAGE_PULL_SECRETS);
        this.flinkConfig.set(
                KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, TESTING_SERVICE_ACCOUNT);

        // Set fixed ports
        flinkConfig.set(RestOptions.PORT, Constants.REST_PORT);
        flinkConfig.set(BlobServerOptions.PORT, Integer.toString(Constants.BLOB_SERVER_PORT));
        flinkConfig.setString(
                TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));

        // Set resources
        flinkConfig.set(
                JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(RESOURCE_MEMORY));
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, RESOURCE_CPU);
        flinkConfig.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(RESOURCE_MEMORY));
        flinkConfig.set(TaskManagerOptions.CPU_CORES, RESOURCE_CPU);
    }

    @Override
    public final void onSetup() throws Exception {
        final FlinkPod podTemplate =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        this.resultPod = getResultPod(podTemplate);
    }

    protected abstract FlinkPod getResultPod(FlinkPod podTemplate) throws Exception;

    @Test
    public void testPodLabelsMerging() {
        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.putAll(userLabels);
        // The label from pod template
        expectedLabels.put("label-key-of-pod-template", "label-value-of-pod-template");

        assertThat(
                new ArrayList<>(
                        this.resultPod
                                .getPodWithoutMainContainer()
                                .getMetadata()
                                .getLabels()
                                .entrySet()),
                hasItems(expectedLabels.entrySet().toArray()));
    }

    @Test
    public void testPodAnnotationsMerging() {
        final Map<String, String> expectedAnnotations = new HashMap<>(ANNOTATIONS);
        // The annotations from pod template
        expectedAnnotations.put(
                "annotation-key-of-pod-template", "annotation-value-of-pod-template");
        assertThat(
                this.resultPod.getPodWithoutMainContainer().getMetadata().getAnnotations(),
                is(equalTo(expectedAnnotations)));
    }

    @Test
    public void testImagePullSecretsMerging() {
        final List<String> expectedPullSecrets = new ArrayList<>(IMAGE_PULL_SECRETS);
        // The image pull secret from pod template
        expectedPullSecrets.add("image-pull-secret-of-pod-template");
        final List<String> resultSecrets =
                this.resultPod.getPodWithoutMainContainer().getSpec().getImagePullSecrets().stream()
                        .map(LocalObjectReference::getName)
                        .collect(Collectors.toList());

        assertThat(resultSecrets, containsInAnyOrder(expectedPullSecrets.toArray()));
    }

    @Test
    public void testNodeSelectorMerging() {
        final Map<String, String> expectedNodeSelectors = new HashMap<>(nodeSelector);
        // The node selector from pod template
        expectedNodeSelectors.put(
                "node-selector-key-of-pod-template", "node-selector-value-of-pod-template");
        assertThat(
                this.resultPod.getPodWithoutMainContainer().getSpec().getNodeSelector(),
                is(equalTo(expectedNodeSelectors)));
    }

    @Test
    public void testPodTolerationsMerging() {
        final List<Toleration> expectedTolerations =
                Arrays.asList(
                        new Toleration("NoSchedule", "key1", "Equal", null, "value1"),
                        // The toleration from pod template
                        new Toleration("NoExecute", "key2-of-pod-template", "Exists", 6000L, null));
        assertThat(
                this.resultPod.getPodWithoutMainContainer().getSpec().getTolerations(),
                Matchers.containsInAnyOrder(expectedTolerations.toArray()));
    }

    @Test
    public void testMainContainerEnvMerging() {
        final List<EnvVar> envVars = this.resultPod.getMainContainer().getEnv();
        final Map<String, String> actualEnvs = new HashMap<>();
        envVars.forEach(env -> actualEnvs.put(env.getName(), env.getValue()));

        final Map<String, String> expectedEnvs = new HashMap<>(customizedEnvs);
        // The envs from pod template
        expectedEnvs.put("ENV_OF_POD_TEMPLATE", "env-value-of-pod-template");

        assertThat(
                new ArrayList<>(actualEnvs.entrySet()),
                hasItems(expectedEnvs.entrySet().toArray()));
    }

    @Test
    public void testMainContainerResourceRequirementsMerging() {
        final ResourceRequirements resourceRequirements =
                this.resultPod.getMainContainer().getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertThat(
                requests.get("cpu").getAmount(),
                is(
                        Double.toString(
                                KubernetesUtils.getRequestCpu(
                                        RESOURCE_CPU,
                                        flinkConfig.getDouble(
                                                KubernetesConfigOptions.CPU_REQUEST_PERCENT)))));
        assertThat(
                requests.get("memory").getAmount(),
                is(
                        String.valueOf(
                                KubernetesUtils.getRequestMem(
                                        RESOURCE_MEMORY,
                                        flinkConfig.getDouble(
                                                KubernetesConfigOptions.MEM_REQUEST_PERCENT)))));
        assertThat(requests.get("ephemeral-storage").getAmount(), is("256"));

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertThat(limits.get("cpu").getAmount(), is(String.valueOf(RESOURCE_CPU)));
        assertThat(limits.get("memory").getAmount(), is(String.valueOf(RESOURCE_MEMORY)));
        assertThat(limits.get("ephemeral-storage").getAmount(), is("256"));
    }

    @Test
    public void testServiceAccountOverwritten() {
        assertThat(
                this.resultPod.getPodWithoutMainContainer().getSpec().getServiceAccountName(),
                is(TESTING_SERVICE_ACCOUNT));
        assertThat(
                this.resultPod.getPodWithoutMainContainer().getSpec().getServiceAccount(),
                is(TESTING_SERVICE_ACCOUNT));
    }

    @Test
    public void testMainContainerImageOverwritten() {
        assertThat(this.resultPod.getMainContainer().getImage(), is(IMAGE));
    }

    @Test
    public void testMainContainerImagePullPolicyOverwritten() {
        assertThat(this.resultPod.getMainContainer().getImagePullPolicy(), is(IMAGE_PULL_POLICY));
    }
}
