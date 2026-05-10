package io.lettuce.core.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.lettuce.core.codec.CRC16;
import io.lettuce.core.codec.RedisCodec;

/**
 * Utility to calculate the slot from a key.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class SlotHash {

    /**
     * Constant for a subkey start.
     */
    public static final byte SUBKEY_START = (byte) '{';

    /**
     * Constant for a subkey end.
     */
    public static final byte SUBKEY_END = (byte) '}';

    /**
     * Number of redis cluster slot hashes.
     */
    public static final int SLOT_COUNT = 16384;

    private SlotHash() {

    }

    /**
     * Calculate the slot from the given key.
     *
     * @param key the key
     * @return slot
     */
    public static int getSlot(String key) {
        return getSlot(key.getBytes());
    }

    /**
     * Calculate the slot from the given key.
     *
     * @param key the key
     * @return slot
     */
    public static int getSlot(byte[] key) {
        return getSlot(ByteBuffer.wrap(key));
    }

    /**
     * Calculate the slot from the given key.
     *
     * @param key the key
     * @return slot
     */
    public static int getSlot(ByteBuffer key) {

        int limit = key.limit();
        int position = key.position();

        int start = indexOf(key, SUBKEY_START);
        if (start != -1) {
            int end = indexOf(key, start + 1, SUBKEY_END);
            if (end != -1 && end != start + 1) {
                key.position(start + 1).limit(end);
            }
        }

        try {
            if (key.hasArray()) {
                return CRC16.crc16(key.array(), key.position(), key.limit() - key.position()) % SLOT_COUNT;
            }
            return CRC16.crc16(key) % SLOT_COUNT;
        } finally {
            key.position(position).limit(limit);
        }
    }

    private static int indexOf(ByteBuffer haystack, byte needle) {
        return indexOf(haystack, haystack.position(), needle);
    }

    private static int indexOf(ByteBuffer haystack, int start, byte needle) {

        for (int i = start; i < haystack.remaining(); i++) {

            if (haystack.get(i) == needle) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Partition keys by slot-hash. The resulting map honors order of the keys.
     *
     * @param codec codec to encode the key.
     * @param keys iterable of keys.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return map between slot-hash and an ordered list of keys.
     */
    public static <K, V, P> Map<Integer, P> partition(RedisCodec<K, V> codec, Iterable<K> keys,
            PartitionElementConsumer<K, P> slotElementConsumer) {

        Map<Integer, P> partitioned = new HashMap<>();

        for (K key : keys) {
            int slot = getSlot(codec.encodeKey(key));
            P partition = partitioned.computeIfAbsent(slot, (K) -> slotElementConsumer.newPartition());
            slotElementConsumer.addElement(partition, key);
        }

        return partitioned;
    }

    public static <K, V> Map<Integer, List<K>> partition(RedisCodec<K, V> codec, Iterable<K> keys) {

        PartitionElementConsumer<K, List<K>> partitionCollectior = new PartitionElementConsumer<K, List<K>>() {

            @Override
            public List<K> newPartition() {
                return new ArrayList<K>(); // LinkedList probably better call here but leave as before
            }

            @Override
            public void addElement(List<K> partition, K key) {
                partition.add(key);
            }

        };
        return partition(codec, keys, partitionCollectior);
    }

    /**
     * Create mapping between the Key and hash slot.
     *
     * @param partitioned map partitioned by slot-hash and keys.
     * @return map of keys to their slot-hash.
     * @param <K> key type
     * @param <S> slot-hash number type.
     */
    public static <S extends Number, K> Map<K, S> getSlots(Map<S, ? extends Iterable<K>> partitioned) {

        Map<K, S> result = new HashMap<>();

        for (Map.Entry<S, ? extends Iterable<K>> entry : partitioned.entrySet()) {
            for (K key : entry.getValue()) {
                result.put(key, entry.getKey());
            }
        }

        return result;
    }

    public interface PartitionElementConsumer<K, P> {

        public P newPartition();

        public void addElement(P partition, K elem);

    }

    public static class OridnalPositionKeys<K> {

        private List<K> partitionKeys;

        private List<Integer> ordinalPosition;

        public OridnalPositionKeys() {
            partitionKeys = new ArrayList<K>();
            ordinalPosition = new ArrayList<Integer>();
        }

        public void addElement(K key, int ordinalPosition) {
            this.partitionKeys.add(key);
            this.ordinalPosition.add(ordinalPosition);
        }

        public List<K> getKeys() {
            return this.partitionKeys;
        }

        public Integer getOrdinalPosition(int partitionPosition) {
            return this.ordinalPosition.get(partitionPosition);
        }

    }

    public static class OrdinalPositionTrackingPartitonElementConsumer<K>
            implements PartitionElementConsumer<K, OridnalPositionKeys<K>> {

        // keeping track here internally or better explicitly in the partition loop?
        private int nextPosition = 0;

        @Override
        public OridnalPositionKeys<K> newPartition() {
            return new OridnalPositionKeys<K>();
        }

        @Override
        public void addElement(OridnalPositionKeys<K> partition, K elem) {
            partition.addElement(elem, nextPosition);
            this.nextPosition++;
        }

        public int totalKeys() {
            return this.nextPosition;
        }

    }

}
