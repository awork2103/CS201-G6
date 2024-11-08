package edu.smu.smusql;

import java.util.*;

public class DoubleHashMap<K, V> {

    private double loadFactor;
    private int hashMultiplier;
    private String hashingStrategy;
    private Entry<K, V>[] table;
    private int size;
    private static final int DEFAULT_CAPACITY = 11;

    public DoubleHashMap(int capacity, double loadFactor, int hashMultiplier, String hashingStrategy) {
        this.loadFactor = loadFactor;
        this.hashMultiplier = hashMultiplier;
        this.hashingStrategy = hashingStrategy;
        this.table = new Entry[capacity];
        this.size = 0;
    }

    public DoubleHashMap() {
        this(DEFAULT_CAPACITY, 0.75, 31, "POLYNOMIAL");
    }

    // Entry to store key-value pairs
    private static class Entry<K, V> {
        K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    // Bitwise Hashing
    private int customBitwiseHashCode(K key) {
        String str = key.toString();
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((int) str.charAt(i));
        }
        return hash;
    }

    // Polynomial Hashing
    private int customPolynomialHashCode(K key) {
        String str = key.toString();
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash *= hashMultiplier;
            hash += (int) str.charAt(i);
        }
        return hash;
    }

    // Cyclic Hashing
    private int customCyclicHashCode(K key) {
        String str = key.toString();
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << 5) | (hash >>> 27);
            hash += (int) str.charAt(i);
        }
        return hash;
    }

    // Additive Hashing
    private int customAdditiveHashCode(K key) {
        String str = key.toString();
        int hash = 0;
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
            default:
                throw new IllegalArgumentException("Unknown hashing strategy");
        }
    }

    // Secondary hash function for double hashing
    private int secondaryHash(K key) {
        // Ensure secondary hash is non-zero, using a simple modulo with a prime smaller than table length
        int secondaryHash = 7 - (Math.abs(customHashCode(key)) % 7);
        return secondaryHash == 0 ? 1 : secondaryHash;
    }

    private int findSlot(K key) {
        int index = Math.abs(customHashCode(key)) % table.length;
        int stepSize = secondaryHash(key); // Calculate step size for double hashing
        int startIndex = index;

        // Double hashing probing logic
        while (table[index] != null && (table[index].key == null || !table[index].key.equals(key))) {
            index = (index + stepSize) % table.length;
            if (index == startIndex) {
                throw new IllegalStateException("HashMap full");
            }
        }
        return index;
    }

    public void put(K key, V value) {
        if ((double) size / table.length > loadFactor) {
            resize();
        }

        int index = findSlot(key);
        if (table[index] == null || table[index].key == null) {
            size++;
        }
        table[index] = new Entry<>(key, value);
    }

    public V get(K key) {
        int index = findSlot(key);
        if (table[index] != null && table[index].key != null && table[index].key.equals(key)) {
            return table[index].value;
        }
        return null;
    }

    public boolean containsKey(K key) {
        int index = findSlot(key);
        return table[index] != null && table[index].key != null && table[index].key.equals(key);
    }

    public V remove(K key) {
        int index = findSlot(key);
        if (table[index] != null && table[index].key != null && table[index].key.equals(key)) {
            V value = table[index].value;
            table[index].key = null;
            table[index].value = null;
            size--;
            return value;
        }
        return null;
    }

    private void resize() {
        Entry<K, V>[] oldTable = table;
        table = new Entry[oldTable.length * 2];
        size = 0;

        for (Entry<K, V> entry : oldTable) {
            if (entry != null && entry.key != null) {
                put(entry.key, entry.value);
            }
        }
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
