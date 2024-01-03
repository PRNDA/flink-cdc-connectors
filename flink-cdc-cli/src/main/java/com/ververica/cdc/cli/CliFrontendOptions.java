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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** Command line argument options for {@link CliFrontend}. */
public class CliFrontendOptions {
    public static final Option FLINK_HOME =
            Option.builder()
                    .longOpt("flink-home")
                    .hasArg()
                    .desc("Path of Flink home directory")
                    .build();

    public static final Option HELP =
            Option.builder("h").longOpt("help").desc("Display help message").build();

    public static final Option GLOBAL_CONFIG =
            Option.builder()
                    .longOpt("global-config")
                    .hasArg()
                    .desc("Path of the global configuration file for Flink CDC pipelines")
                    .build();

    public static final Option JAR =
            Option.builder()
                    .longOpt("jar")
                    .hasArgs()
                    .desc("JARs to be submitted together with the pipeline")
                    .build();

    public static final Option USE_MINI_CLUSTER =
            Option.builder()
                    .longOpt("use-mini-cluster")
                    .hasArg(false)
                    .desc("Use Flink MiniCluster to run the pipeline")
                    .build();

    public static final Option SAVEPOINT_PATH_OPTION =
            Option.builder("s")
                    .longOpt("fromSavepoint")
                    .hasArg(true)
                    .desc(
                            "Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537")
                    .build();

    public static final Option SAVEPOINT_RESTORE_MODE =
            Option.builder("rm")
                    .longOpt("restoreMode")
                    .hasArg(true)
                    .desc(
                            "Defines how should we restore from the given savepoint. Supported options: "
                                    + "[claim - claim ownership of the savepoint and delete once it is"
                                    + " subsumed, no_claim (default) - do not claim ownership, the first"
                                    + " checkpoint will not reuse any files from the restored one, legacy "
                                    + "- the old behaviour, do not assume ownership of the savepoint files,"
                                    + " but can reuse some shared files")
                    .build();

    public static Options initializeOptions() {
        return new Options()
                .addOption(HELP)
                .addOption(JAR)
                .addOption(FLINK_HOME)
                .addOption(GLOBAL_CONFIG)
                .addOption(USE_MINI_CLUSTER)
                .addOption(SAVEPOINT_PATH_OPTION)
                .addOption(SAVEPOINT_RESTORE_MODE);
    }
}
