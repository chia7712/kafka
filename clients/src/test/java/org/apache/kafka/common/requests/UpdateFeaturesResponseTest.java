/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateFeaturesResponseTest {

    @Test
    public void testErrorCounts() {
        UpdateFeaturesResponseData.UpdatableFeatureResultCollection results =
            new UpdateFeaturesResponseData.UpdatableFeatureResultCollection();

        results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult()
            .setFeature("foo")
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
        );

        results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult()
            .setFeature("bar")
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
        );

        results.add(new UpdateFeaturesResponseData.UpdatableFeatureResult()
            .setFeature("baz")
            .setErrorCode(Errors.FEATURE_UPDATE_FAILED.code())
        );

        UpdateFeaturesResponse response = new UpdateFeaturesResponse(new UpdateFeaturesResponseData()
            .setErrorCode(Errors.INVALID_REQUEST.code())
            .setResults(results)
        );

        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertEquals(3, errorCounts.size());
        assertEquals(1, errorCounts.get(Errors.INVALID_REQUEST).intValue());
        assertEquals(2, errorCounts.get(Errors.UNKNOWN_SERVER_ERROR).intValue());
        assertEquals(1, errorCounts.get(Errors.FEATURE_UPDATE_FAILED).intValue());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.UPDATE_FEATURES)
    public void testSerialization(short version) {
        UpdateFeaturesResponse noErrorResponse = UpdateFeaturesResponse.parse(UpdateFeaturesResponse.createWithErrors(ApiError.NONE,
            Set.of("feature-1", "feature-2"), 0).serialize(version), version);

        // Versions 1 and below still contain feature level results when the error is NONE.
        int expectedSize = version <= 1 ? 2 : 0;
        assertEquals(ApiError.NONE, noErrorResponse.topLevelError());
        assertEquals(expectedSize, noErrorResponse.data().results().size());

        ApiError error = new ApiError(Errors.INVALID_UPDATE_VERSION);
        UpdateFeaturesResponse errorResponse = UpdateFeaturesResponse.parse(UpdateFeaturesResponse.createWithErrors(error,
            Set.of("feature-1", "feature-2"), 0).serialize(version), version);
        assertEquals(error, errorResponse.topLevelError());
        assertEquals(0, errorResponse.data().results().size());
    }
}
