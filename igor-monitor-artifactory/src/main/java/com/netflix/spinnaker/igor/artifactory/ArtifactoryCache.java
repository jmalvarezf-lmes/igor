/*
 * Copyright 2019 Pivotal, Inc.
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

package com.netflix.spinnaker.igor.artifactory;

import com.netflix.spinnaker.igor.IgorConfigurationProperties;
import com.netflix.spinnaker.igor.artifactory.model.ArtifactoryRepositoryType;
import com.netflix.spinnaker.igor.artifactory.model.ArtifactorySearch;
import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import com.netflix.spinnaker.kork.jedis.RedisClientDelegate;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class ArtifactoryCache {
  private static final String POLLING_ID = "artifactory:publish:queue";

  private static final String POLL_STAMP = "lastPollCycleTimestamp";

  private static final String LOCATION = "location";

  private static final String ARTIFACT_REPO_NAME = "artifactory";

  private final RedisClientDelegate redisClientDelegate;
  private final IgorConfigurationProperties igorConfigurationProperties;

  public void setLastPollCycleTimestamp(ArtifactorySearch search, long timestamp) {
    String key = makePollingKey(search);
    redisClientDelegate.withCommandsClient(
        c -> {
          c.hset(key, POLL_STAMP, Long.toString(timestamp));
        });
  }

  public void setArtifactKey(Artifact item, ArtifactoryRepositoryType type) {
    String key = makeArtifactKey(item, type);
    redisClientDelegate.withCommandsClient(
        c -> {
          c.hset(key, LOCATION, item.getLocation());
        });
  }

  public Long getLastPollCycleTimestamp(ArtifactorySearch search) {
    return redisClientDelegate.withCommandsClient(
        c -> {
          String ts = c.hget(makePollingKey(search), POLL_STAMP);
          return ts == null ? null : Long.parseLong(ts);
        });
  }

  private String makePollingKey(ArtifactorySearch search) {
    return pollingPrefix() + ":" + search.getPartitionName() + ":" + search.getGroupId();
  }

  private String makeArtifactKey(Artifact item, ArtifactoryRepositoryType type) {
    return artifactPrefix()
        + ":"
        + type.getRepoTypeString()
        + ":"
        + ARTIFACT_REPO_NAME
        + ":"
        + item.getProvenance()
        + ":"
        + item.getReference();
  }

  private String pollingPrefix() {
    return igorConfigurationProperties.getSpinnaker().getJedis().getPrefix() + ":" + POLLING_ID;
  }

  private String artifactPrefix() {
    return igorConfigurationProperties.getSpinnaker().getJedis().getPrefix();
  }
}
