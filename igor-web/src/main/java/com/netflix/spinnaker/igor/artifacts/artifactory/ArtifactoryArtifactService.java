/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.spinnaker.igor.artifacts.artifactory;

import static org.jfrog.artifactory.client.aql.AqlItem.aqlItem;

import com.netflix.spinnaker.igor.artifactory.model.ArtifactorySearch;
import com.netflix.spinnaker.igor.artifacts.ArtifactService;
import com.netflix.spinnaker.igor.artifacts.artifactory.model.ArtifactoryQueryResults;
import com.netflix.spinnaker.igor.config.ArtifactoryProperties;
import com.netflix.spinnaker.igor.model.ArtifactServiceProvider;
import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jfrog.artifactory.client.Artifactory;
import org.jfrog.artifactory.client.ArtifactoryClientBuilder;
import org.jfrog.artifactory.client.ArtifactoryRequest;
import org.jfrog.artifactory.client.ArtifactoryResponse;
import org.jfrog.artifactory.client.aql.AqlQueryBuilder;
import org.jfrog.artifactory.client.impl.ArtifactoryRequestImpl;
import org.springframework.stereotype.Service;

@Slf4j
@Service("artifactory")
public class ArtifactoryArtifactService implements ArtifactService {

  private ArtifactoryProperties artifactoryProperties;

  public ArtifactoryArtifactService(ArtifactoryProperties artifactoryProperties) {
    this.artifactoryProperties = artifactoryProperties;
  }

  @NotNull
  @Override
  public ArtifactServiceProvider artifactServiceProvider() {
    return ArtifactServiceProvider.ARTIFACTORY;
  }

  @NotNull
  @Override
  public List<String> getArtifactVersions(
      @NotNull String type, @NotNull String name, List<String> releaseStatuses) {
    return artifactoryProperties.getSearches().stream()
        .map(
            search -> {
              List<String> includes = new ArrayList<>(Arrays.asList("path", "repo", "name"));

              AqlQueryBuilder aqlQueryBuilder =
                  new AqlQueryBuilder()
                      .item(aqlItem("repo", search.getRepo()))
                      .item(aqlItem("name", aqlItem("$match", "*" + type)))
                      .item(aqlItem("path", aqlItem("$match", name.replaceAll(":", "/") + "*")))
                      .include(includes.toArray(new String[0]));

              try {
                ArtifactoryResponse aqlResponse =
                    this.artifactoryQuery(search, aqlQueryBuilder.build());
                if (aqlResponse.isSuccessResponse()) {
                  return aqlResponse.parseBody(ArtifactoryQueryResults.class).getResults().stream()
                      .map(
                          item ->
                              item.toMatchableArtifact(search.getRepoType(), search.getBaseUrl())
                                  .getVersion())
                      .collect(Collectors.toList());
                }
                log.warn(
                    "Unable to query Artifactory for artifacts (HTTP {}): {}",
                    aqlResponse.getStatusLine().getStatusCode(),
                    aqlResponse.getRawBody());
                return Collections.EMPTY_LIST;
              } catch (IOException e) {
                log.warn("Unable to query Artifactory for artifacts", e);
                return Collections.EMPTY_LIST;
              }
            })
        .collect(ArrayList<String>::new, List::addAll, List::addAll);
  }

  @NotNull
  @Override
  public Artifact getArtifact(@NotNull String type, @NotNull String name, @NotNull String version) {
    return artifactoryProperties.getSearches().stream()
        .map(
            search -> {
              List<String> includes = new ArrayList<>(Arrays.asList("path", "repo", "name"));

              AqlQueryBuilder aqlQueryBuilder =
                  new AqlQueryBuilder()
                      .item(aqlItem("repo", search.getRepo()))
                      .item(aqlItem("name", aqlItem("$match", "*" + type)))
                      .item(
                          aqlItem(
                              "path", aqlItem("$match", name.replaceAll(":", "/") + "/" + version)))
                      .include(includes.toArray(new String[0]));

              try {
                ArtifactoryResponse aqlResponse =
                    this.artifactoryQuery(search, aqlQueryBuilder.build());
                if (aqlResponse.isSuccessResponse()) {
                  return aqlResponse.parseBody(ArtifactoryQueryResults.class).getResults().stream()
                      .map(
                          item ->
                              item.toMatchableArtifact(search.getRepoType(), search.getBaseUrl()))
                      .collect(Collectors.toList());
                }
                log.warn(
                    "Unable to query Artifactory for artifacts (HTTP {}): {}",
                    aqlResponse.getStatusLine().getStatusCode(),
                    aqlResponse.getRawBody());
                return Collections.EMPTY_LIST;
              } catch (IOException e) {
                log.warn("Unable to query Artifactory for artifacts", e);
                return Collections.EMPTY_LIST;
              }
            })
        .collect(ArrayList<Artifact>::new, List::addAll, List::addAll)
        .get(0);
  }

  private ArtifactoryResponse artifactoryQuery(ArtifactorySearch search, String query)
      throws IOException {
    Artifactory client =
        ArtifactoryClientBuilder.create()
            .setUsername(search.getUsername())
            .setPassword(search.getPassword())
            .setAccessToken(search.getAccessToken())
            .setUrl(search.getBaseUrl())
            .setIgnoreSSLIssues(search.isIgnoreSslIssues())
            .build();

    ArtifactoryRequest aqlRequest =
        new ArtifactoryRequestImpl()
            .method(ArtifactoryRequest.Method.POST)
            .apiUrl("api/search/aql")
            .requestType(ArtifactoryRequest.ContentType.TEXT)
            .responseType(ArtifactoryRequest.ContentType.JSON)
            .requestBody(query);

    return client.restCall(aqlRequest);
  }
}
