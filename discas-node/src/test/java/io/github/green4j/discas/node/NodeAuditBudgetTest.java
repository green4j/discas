/*
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for details.
 */

package io.github.green4j.discas.node;

import io.github.green4j.discas.common.identity.ClusterId;
import io.github.green4j.discas.common.identity.NodeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("NodeConfig -- the store and the audit buffer share one heap budget")
class NodeAuditBudgetTest {

    private static NodeConfig.Builder builder() {
        return NodeConfig.builder()
                .nodeId(NodeId.of("n1"))
                .clusterId(ClusterId.of("c"))
                .clusterSize(3);
    }

    @Test
    void theBufferComesOutOfTheStoreCapacity() {
        final int buffer = 1 << 20;
        final NodeConfig withAudit = builder().auditBufferBytes(buffer).build();

        assertEquals(withAudit.heapBudgetBytes() - buffer, withAudit.storeCapacityBytes());
        assertEquals(buffer, withAudit.auditBufferBytes());
    }

    @Test
    void withoutAuditTheStoreKeepsTheWholeBudget() {
        final NodeConfig plain = builder().build();

        assertEquals(0, plain.auditBufferBytes());
        assertEquals(plain.heapBudgetBytes(), plain.storeCapacityBytes());
    }

    @Test
    void refusesABufferThatIsNotAPowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> builder().auditBufferBytes(5000).build());
    }

    @Test
    @DisplayName("A buffer past a quarter of the budget is refused, not silently taken")
    void refusesABufferThatCrowdsOutTheStore() {
        final long budget = builder().build().heapBudgetBytes();
        int tooBig = 1 << 20;
        while (tooBig <= budget / 4) {
            tooBig <<= 1;
        }
        final int size = tooBig;
        assertTrue(size > budget / 4);
        assertThrows(IllegalArgumentException.class, () -> builder().auditBufferBytes(size).build());
    }
}
