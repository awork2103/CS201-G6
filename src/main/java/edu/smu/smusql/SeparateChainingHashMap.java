package edu.smu.smusql;

import java.util.*;

// Class is meant to replace HashMap in both CustomHashMap and HashMapPlusTree
class SeparateChainingHashMap<K, V> {

    private double loadFactor;
    private int hashMultiplier;
    // Values of hashingStrategy can be BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
    private String hashingStrategy;
    private LinkedList<Entry<K, V>>[] buckets;
    private int size;

    // Set an initial capacity and loadFactor
    @SuppressWarnings("unchecked")
    public SeparateChainingHashMap(int capacity, double loadFactor, int hashMultiplier, String hashingStrategy) {
        this.loadFactor = loadFactor;
        this.hashMultiplier = hashMultiplier;
        this.hashingStrategy = hashingStrategy;
        buckets = new LinkedList[capacity];
        size = 0;
    }

    // Node to store key-value pairs for HashMap Entries in Separate Chaining
    private static class Entry<K, V> {
        K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public boolean containsKey(K key) {
        int bucketIndex = getBucketIndex(key);

        if (buckets[bucketIndex] != null) {
            for (Entry<K, V> entry : buckets[bucketIndex]) {
                if (entry.key.equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Default Hashing (for Eclipse JDK)
    private int defaultHashCode(K key) {
        return key.hashCode();
    }

    // Bitwise Hashing
    private int customBitwiseHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((int) str.charAt(i));
        }

        return hash;
    }

    // Polynomial Hashing
    private int customPolynomialHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash *= hashMultiplier;
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Cyclic Hashing
    private int customCyclicHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << 5) | (hash >>> 27);
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Additive Hahsing --> Similar to polynomial but without pow(multiplier) by i
    private int customAdditiveHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash += hashMultiplier * (int) str.charAt(i);
        }

        return hash;
    }

    private int customHashCode(K key) {
        switch (hashingStrategy) {
            case "BITWISE":
                return customBitwiseHashCode(key);
            case "POLYNOMIAL":
                return customPolynomialHashCode(key);
            case "CYCLIC":
                return customCyclicHashCode(key);
            case "ADDITIVE":
                return customAdditiveHashCode(key);
            case "DEFAULT":
                return defaultHashCode(key);
            default:
                throw new IllegalArgumentException("Unknown hashing strategy");
        }
    }

    private int getBucketIndex(K key) {
        return Math.abs(customHashCode(key) % buckets.length);
    }

    public void put(K key, V value) {
        int bucketIndex = getBucketIndex(key);

        if (buckets[bucketIndex] == null) {
            buckets[bucketIndex] = new LinkedList<>();
        }

        // Check if key already exists, update the value
        for (Entry<K, V> entry : buckets[bucketIndex]) {
            if (entry.key.equals(key)) {
                entry.value = value;
                return;
            }
        }

        // If key is new, add it
        buckets[bucketIndex].add(new Entry<>(key, value));
        size++;

        // Resize if load factor exceeded
        if ((float) size / buckets.length > loadFactor) {
            resize();
        }
    }

    public V get(K key) {
        int bucketIndex = getBucketIndex(key);

        if (buckets[bucketIndex] != null) {
            for (Entry<K, V> entry : buckets[bucketIndex]) {
                if (entry.key.equals(key)) {
                    return entry.value;
                }
            }
        }
        return null;
    }

    public V getOrDefault(K key, V defaultValue) {
        V value = get(key);
        return value == null ? defaultValue : value;
    }

    public Set<K> keySet() {
        Set<K> keySet = new HashSet<>();
        // Check all buckets in bucketArr
        for (LinkedList<Entry<K, V>> bucket : buckets) {
            if (bucket != null) {
                // Check all entries in bucket (yes For:Each can be used for LinkedList XD)
                for (Entry<K, V> entry : bucket) {
                    keySet.add(entry.key);
                }
            }
        }
        return keySet;
    }

    public V remove(K key) {
        int bucketIndex = getBucketIndex(key);

        if (buckets[bucketIndex] != null) {
            for (Entry<K, V> entry : buckets[bucketIndex]) {
                if (entry.key.equals(key)) {
                    V value = entry.value;
                    buckets[bucketIndex].remove(entry);
                    size--;
                    return value;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void resize() {
        LinkedList<Entry<K, V>>[] oldBuckets = buckets;
        buckets = new LinkedList[buckets.length * 2];

        // Transfer all buckets from old arr to new arr (bigger)
        for (LinkedList<Entry<K, V>> bucket : oldBuckets) {
            if (bucket != null) {
                for (Entry<K, V> entry : bucket) {
                    put(entry.key, entry.value);
                }
            }
        }
    }

    public int size() {
        return size;
    }
}
