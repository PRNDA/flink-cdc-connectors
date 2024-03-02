/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.cli;

import org.apache.flink.runtime.jobgraph.RestoreMode;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import com.ververica.cdc.composer.PipelineComposer;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CliFrontendTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @BeforeEach
    void takeoverOutput() {
        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));
    }

    @Test
    void testNoArgument() throws Exception {
        CliFrontend.main(new String[] {});
        assertThat(out.toString()).isEqualTo(HELP_MESSAGE);
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void testGeneratingHelpMessage() throws Exception {
        CliFrontend.main(new String[] {"--help"});
        assertThat(out.toString()).isEqualTo(HELP_MESSAGE);
        assertThat(err.toString()).isEmpty();
    }

    @Test
    void testMissingFlinkHome() {
        assertThatThrownBy(() -> CliFrontend.main(new String[] {pipelineDef()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Cannot find Flink home from either command line arguments \"--flink-home\" "
                                + "or the environment variable \"FLINK_HOME\". "
                                + "Please make sure Flink home is properly set. ");
    }

    @Test
    void testGlobalPipelineConfigParsing() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "--global-config",
                        globalPipelineConfig());
        assertThat(executor.getGlobalPipelineConfig().toMap().get("parallelism")).isEqualTo("1");
        assertThat(executor.getGlobalPipelineConfig().toMap().get("foo")).isEqualTo("bar");
    }

    @Test
    void testSavePointConfiguration() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "-s",
                        flinkHome() + "/savepoints/savepoint-1",
                        "-cm",
                        "no_claim",
                        "-n");
        assertThat(executor.getSavepointSettings().getRestorePath())
                .isEqualTo(flinkHome() + "/savepoints/savepoint-1");
        assertThat(executor.getSavepointSettings().getRestoreMode())
                .isEqualTo(RestoreMode.NO_CLAIM);
        assertThat(executor.getSavepointSettings().allowNonRestoredState()).isTrue();
    }

    @Test
    void testAdditionalJar() throws Exception {
        String aJar = "/foo/jar/a.jar";
        String bJar = "/foo/jar/b.jar";
        CliExecutor executor =
                createExecutor(
                        pipelineDef(), "--flink-home", flinkHome(), "--jar", aJar, "--jar", bJar);
        assertThat(executor.getAdditionalJars()).contains(Paths.get(aJar), Paths.get(bJar));
    }

    @Test
    void testPipelineExecuting() throws Exception {
        CliExecutor executor =
                createExecutor(
                        pipelineDef(),
                        "--flink-home",
                        flinkHome(),
                        "--global-config",
                        globalPipelineConfig());
        NoOpComposer composer = new NoOpComposer();
        executor.setComposer(composer);
        PipelineExecution.ExecutionInfo executionInfo = executor.run();
        assertThat(executionInfo.getId()).isEqualTo("fake-id");
        assertThat(executionInfo.getDescription()).isEqualTo("fake-description");
    }

    private CliExecutor createExecutor(String... args) throws Exception {
        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        return CliFrontend.createExecutor(parser.parse(cliOptions, args));
    }

    private String pipelineDef() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private String flinkHome() throws Exception {
        URL resource = Resources.getResource("flink-home");
        return Paths.get(resource.toURI()).toString();
    }

    private String globalPipelineConfig() throws Exception {
        URL resource = Resources.getResource("global-config/global-config.yaml");
        return Paths.get(resource.toURI()).toString();
    }

    private static final String HELP_MESSAGE =
            "usage:\n"
                    + "    -cm,--claimMode <arg>        Defines how should we restore from the given\n"
                    + "                                 savepoint. Supported options: [claim - claim\n"
                    + "                                 ownership of the savepoint and delete once it\n"
                    + "                                 is subsumed, no_claim (default) - do not claim\n"
                    + "                                 ownership, the first checkpoint will not reuse\n"
                    + "                                 any files from the restored one, legacy - the\n"
                    + "                                 old behaviour, do not assume ownership of the\n"
                    + "                                 savepoint files, but can reuse some shared\n"
                    + "                                 files\n"
                    + "       --flink-home <arg>        Path of Flink home directory\n"
                    + "       --global-config <arg>     Path of the global configuration file for Flink\n"
                    + "                                 CDC pipelines\n"
                    + "    -h,--help                    Display help message\n"
                    + "       --jar <arg>               JARs to be submitted together with the pipeline\n"
                    + "    -n,--allowNonRestoredState   Allow to skip savepoint state that cannot be\n"
                    + "                                 restored. You need to allow this if you removed\n"
                    + "                                 an operator from your program that was part of\n"
                    + "                                 the program when the savepoint was triggered.\n"
                    + "    -s,--fromSavepoint <arg>     Path to a savepoint to restore the job from\n"
                    + "                                 (for example hdfs:///flink/savepoint-1537\n"
                    + "       --use-mini-cluster        Use Flink MiniCluster to run the pipeline\n";

    private static class NoOpComposer implements PipelineComposer {

        @Override
        public PipelineExecution compose(PipelineDef pipelineDef) {
            return () -> new PipelineExecution.ExecutionInfo("fake-id", "fake-description");
        }
    }
}
