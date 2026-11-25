/*
 * Derived from io.openmessaging.benchmark.utils.distributor.KeyRoundRobin
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark)
 * and modified for Dimster.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.dimster.utils.distributor;

/**
 * Cycles through the shared {@link MessageDistributor#RANDOM_KEYS} pool in
 * order. Each producer thread maintains its own cursor via {@link ThreadLocal}
 * so concurrent producers do not bounce a shared cache line on every send, and
 * returns pre-allocated {@link MessageDistributor#KEY_TARGETS} with zero
 * per-message allocation.
 */
public final class KeyRoundRobin implements MessageDistributor {

    // Stores a 1-element int[] per thread (no back-reference to KeyRoundRobin).
    private final ThreadLocal<int[]> cursor = ThreadLocal.withInitial(() -> new int[1]);

    @Override
    public Target next() {
        int[] box = cursor.get();
        int idx = box[0]++;
        if (idx >= UNIQUE_COUNT) {
            idx = 0;
            box[0] = 1;
        }
        return KEY_TARGETS[idx];
    }
}
