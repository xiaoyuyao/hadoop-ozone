/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RocksDBCheckpoint;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.ha.OMNodeDetails;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import org.apache.commons.io.FileUtils;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_INDEX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_TERM;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY;

import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OzoneManagerSnapshotProvider downloads the latest checkpoint from the
 * leader OM and loads the checkpoint into State Machine.
 */
public class OzoneManagerSnapshotProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerSnapshotProvider.class);

  private final File omSnapshotDir;
  private Map<String, OMNodeDetails> peerNodesMap;
  private final HttpConfig.Policy httpPolicy;
  private final boolean spnegoEnabled;
  private final URLConnectionFactory connectionFactory;

  private static final String OM_SNAPSHOT_DB = "om.snapshot.db";

  public OzoneManagerSnapshotProvider(ConfigurationSource conf,
      File omRatisSnapshotDir, List<OMNodeDetails> peerNodes) {

    LOG.info("Initializing OM Snapshot Provider");
    this.omSnapshotDir = omRatisSnapshotDir;

    this.peerNodesMap = new HashMap<>();
    for (OMNodeDetails peerNode : peerNodes) {
      this.peerNodesMap.put(peerNode.getOMNodeId(), peerNode);
    }

    this.httpPolicy = HttpConfig.getHttpPolicy(conf);
    this.spnegoEnabled = conf.get(OZONE_OM_HTTP_AUTH_TYPE, "simple")
        .equals("kerberos");

    TimeUnit connectionTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getUnit();
    int connectionTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT.getDuration(),
        connectionTimeoutUnit);

    TimeUnit requestTimeoutUnit =
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getUnit();
    int requestTimeoutMS = (int) conf.getTimeDuration(
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY,
        OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT.getDuration(),
        requestTimeoutUnit);

    connectionFactory = URLConnectionFactory
      .newDefaultURLConnectionFactory(connectionTimeoutMS, requestTimeoutMS,
            LegacyHadoopConfigurationSource.asHadoopConfiguration(conf));
  }

  /**
   * Download the latest checkpoint from OM Leader via HTTP.
   * @param leaderOMNodeID leader OM Node ID.
   * @return the DB checkpoint (including the ratis snapshot index)
   */
  public DBCheckpoint getOzoneManagerDBSnapshot(String leaderOMNodeID)
      throws IOException {
    String snapshotFileName = OM_SNAPSHOT_DB + "_" + System.currentTimeMillis();
    File targetFile = new File(omSnapshotDir, snapshotFileName + ".tar.gz");

    String omCheckpointUrl = peerNodesMap.get(leaderOMNodeID)
        .getOMDBCheckpointEnpointUrl(httpPolicy);

    LOG.info("Downloading latest checkpoint from Leader OM {}. Checkpoint " +
        "URL: {}", leaderOMNodeID, omCheckpointUrl);
    final long[] snapshotIndex = new long[1];
    final long[] snapshotTerm = new long[1];
    SecurityUtil.doAsCurrentUser(() -> {
      HttpURLConnection httpURLConnection = (HttpURLConnection)
          connectionFactory.openConnection(new URL(omCheckpointUrl),
              spnegoEnabled);
      httpURLConnection.connect();
      int errorCode = httpURLConnection.getResponseCode();
      if ((errorCode != HTTP_OK) && (errorCode != HTTP_CREATED)) {
        throw new IOException("Unexpected exception when trying to reach " +
            "OM to download latest checkpoint. Checkpoint URL: " +
            omCheckpointUrl + ". ErrorCode: " + errorCode);
      }
      snapshotIndex[0] = httpURLConnection.getHeaderFieldLong(
          OM_RATIS_SNAPSHOT_INDEX, -1);
      if (snapshotIndex[0] == -1) {
        throw new IOException("The HTTP response header " +
            OM_RATIS_SNAPSHOT_INDEX + " is missing.");
      }
      snapshotTerm[0] = httpURLConnection.getHeaderFieldLong(
          OM_RATIS_SNAPSHOT_TERM, -1);
      if (snapshotTerm[0] == -1) {
        throw new IOException("The HTTP response header " +
            OM_RATIS_SNAPSHOT_TERM + " is missing.");
      }

      try (InputStream inputStream = httpURLConnection.getInputStream()) {
        FileUtils.copyInputStreamToFile(inputStream, targetFile);
      }
      return null;
    });
    // Untar the checkpoint file.
    Path untarredDbDir = Paths.get(omSnapshotDir.getAbsolutePath(),
        snapshotFileName);
    FileUtil.unTar(targetFile, untarredDbDir.toFile());
    FileUtils.deleteQuietly(targetFile);

    LOG.info("Sucessfully downloaded latest checkpoint with snapshot " +
        "index {} from leader OM: {}", snapshotIndex[0], leaderOMNodeID);

    RocksDBCheckpoint omCheckpoint = new RocksDBCheckpoint(untarredDbDir);
    omCheckpoint.setRatisSnapshotIndex(snapshotIndex[0]);
    omCheckpoint.setRatisSnapshotTerm(snapshotTerm[0]);
    return omCheckpoint;
  }
}
