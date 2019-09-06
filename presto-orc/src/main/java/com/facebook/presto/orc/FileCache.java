/*
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

package com.facebook.presto.orc;

import com.facebook.presto.spi.memory.ArrayPool;
import com.facebook.presto.spi.memory.Caches;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
    import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileCache
{
    private static final long MAX_PREFETCH_SIZE = 4L << 30;
    private static final int MAX_ENTRIES = 200000;
    // Constant from MurMur hash.
    private static final long M = 0xc6a4a7935bd1e995L;
    private static final Logger log = Logger.get(FileCache.class);

    private static final List<WeakReference<FileToken>>[] fileTokens = new ArrayList[1024];
    private static final List<Entry>[] entryHashTable = new ArrayList[8192];
    private static final Entry[] entries = new Entry[MAX_ENTRIES];
    private static final ReferenceQueue<byte[]> gcdBuffers = new ReferenceQueue();
    private static volatile int clockHand;
    private static int numEntries;
    private static ArrayPool<byte[]> byteArrayPool;
    // Anything with age > evictionThreshold is evictable.
    private static long evictionThreshold = Long.MIN_VALUE;
    // numGets at the time evictionThreshold was last computed.
    private static long numGetsForStats;
    // Place in entries where to start the next sampling.
    private static int statsClockHand;
    private static long statsTime;
    private static boolean useThreshold = true;
    // True until being at capacity for the first time.
    private static boolean initializing = true;

    static {
        byteArrayPool = Caches.getByteArrayPool();
        for (int i = 0; i < 200; i++) {
            entries[numEntries++] = new Entry(0);
        }
    }

    private static AtomicLong totalSize = new AtomicLong();
    private static AtomicLong prefetchSize = new AtomicLong();
    private static long targetSize = 20 * (1L << 30);
    private static long bytesAllocated;
    private static long numGcdBuffers;
    private static long numGcdBytes;
    private static long numGets;
    private static long numHits;
    private static long numHitBytes;
    private static long numEvicts;
    private static long sumEvictAge;
    private static long numPrefetch;
    private static long prefetchBytes;
    private static long numPrefetchRead;
    private static long prefetchReadBytes;
    private static long numLatePrefetch;
    private static long numWastedPrefetch;
    private static long wastedPrefetchBytes;
    private static long numConcurrentMiss;
    private static long numAllInUse;
    private static long prefetchMicros;
    private static long operatorReadMicros;
    private static long numOperatorRead;
    private static long operatorReadBytes;
    private static long readWaitMicros;

    private static ExecutorService prefetchExecutor;
    private static FileCacheStats statsBean;

    // LRU cache from table.column to per column-wise access listener.
    private static final LoadingCache<String, Listener> listeners =
            CacheBuilder.newBuilder()
            .maximumSize(10000)
            .removalListener(new RemovalListener<String, Listener>() {
                public void onRemoval(RemovalNotification<String, Listener> notification)
                {
                    defaultListener.merge(notification.getValue());
                }
            })
            .build(CacheLoader.from(key -> new Listener(key)));
    private static final Listener defaultListener = new Listener("unspecified");

    private FileCache() {}

    public static class FileToken
    {
        private final String path;
        private final int hash;

        public FileToken(String path)
        {
            this.path = path;
            hash = path.hashCode();
        }

        public boolean matches(String path, long hash)
        {
            return this.hash == hash && this.path.equals(path);
        }

        @Override
            public int hashCode()
        {
            return hash;
        }

        @Override
            public boolean equals(Object other)
        {
            return this == other;
        }

        @Override
        public String toString()
        {
            return "FileToken:" + path;
        }
    }

    // Returns a FileToken for path. Each path has exactly one FileToken.
    public static FileToken getFileToken(String path)
    {
        int hash = path.hashCode();
        int index = hash & (fileTokens.length - 1);
        List<WeakReference<FileToken>> list = fileTokens[index];
        if (list == null) {
            synchronized (FileToken.class) {
                list = fileTokens[index];
                if (list == null) {
                    list = new ArrayList();
                    fileTokens[index] = list;
                }
            }
        }
        synchronized (list) {
            for (int i = 0; i < list.size(); i++) {
                WeakReference<FileToken> reference = list.get(i);
                FileToken token = reference.get();
                if (token == null) {
                    list.remove(i);
                    i--;
                    continue;
                }
                if (token.matches(path, hash)) {
                    return token;
                }
            }
            FileToken token = new FileToken(path);
            list.add(new WeakReference(token));
            return token;
        }
    }

    public static class Listener
    {
        private final String label;
        private final AtomicLong numHits = new AtomicLong();
        private final AtomicLong hitSize = new AtomicLong();
        private final AtomicLong numMisses = new AtomicLong();
        private final AtomicLong missSize = new AtomicLong();
        private final AtomicLong size = new AtomicLong();

        public Listener(String label)
        {
            this.label = label;
        }

        public void loaded(Entry entry)
        {
            size.addAndGet(entry.dataSize);
            missSize.addAndGet(entry.dataSize);
            numMisses.addAndGet(1);
        }

        public void hit(Entry entry)
        {
            numHits.addAndGet(1);
            hitSize.addAndGet(entry.dataSize);
        }

        public void evicted(Entry entry, long now, boolean wasPrefetch)
        {
            size.addAndGet(-entry.dataSize);
        }

        public void merge(Listener other)
        {
            size.addAndGet(other.size.get());
            numHits.addAndGet(other.numHits.get());
            hitSize.addAndGet(other.hitSize.get());
            numMisses.addAndGet(other.numMisses.get());
            missSize.addAndGet(other.missSize.get());
        }

        // The one with the larger hitVolume comes first.
        public int compare(Listener other)
        {
            return hitSize.get() > other.hitSize.get() ? -1 : 1;
        }

        @Override
        public int hashCode()
        {
            return label.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            return this == other;
        }

        @Override
        public String toString()
        {
            return label + " size " + size.get() + " hits " + numHits.get() + " (" + (hitSize.get() / (1L << 20)) + "MB) " + " misses " + numMisses.get() + " (" + (missSize.get() / (1L << 20)) + "MB) ";
        }
    }

    public static Listener getListener(String label)
    {
        if (label == null) {
            return defaultListener;
        }
        try {
            return listeners.get(label);
        }
        catch (Exception e) {
            return defaultListener;
        }
    }

    private static class BufferReference
            extends SoftReference<byte[]>
    {
        private final int bucketIndex;

        BufferReference(byte[] buffer, ReferenceQueue queue, int bucketIndex)
        {
            super(buffer, queue);
            this.bucketIndex = bucketIndex;
        }

        public int getBucket()
        {
            return bucketIndex;
        }
    }

    public static class Entry
            implements Closeable
    {
        FileToken token;
        long offset;
        BufferReference softBuffer;
        byte[] buffer;
        SettableFuture<Boolean> loadingFuture;
        Thread loadingThread;
        Listener listener;
        int dataSize;
        int bufferSize;
        long accessTime;
        volatile int pinCount;
        int accessCount;
        // Controls the rate at which an entry ages. 100 means it ages at the speed of wall time. 1000 means that it ages 10x slower than wall time. This allows preferential retention of high value entries.
        int retentionWeight = 100;
        // When finding an Entry with isTemporary set, wait for the loading future and then retry the get. A temporary entry must not be returned to a caller.
        final boolean isTemporary;
        boolean isPrefetch;
        private byte sizeIndex;

        Entry(int pinCount)
        {
            this.pinCount = pinCount;
            isTemporary = false;
        }

        Entry(boolean isTemporary)
        {
            this.isTemporary = isTemporary;
        }

        public static Entry createTemporary(FileToken token, long offset, int dataSize, boolean isPrefetch)
        {
            Entry entry = new Entry(true);
            entry.token = token;
            entry.dataSize = dataSize;
            entry.offset = offset;
            entry.isPrefetch = isPrefetch;
            entry.loadingFuture = SettableFuture.create();
            entry.loadingThread = currentThread();
            return entry;
        }

        public byte[] getBuffer()
        {
            return buffer;
        }

        @Override
        public void close()
        {
            if (isTemporary) {
                return;
            }
            synchronized (entryHashTable[softBuffer.getBucket()]) {
                if (pinCount <= 0) {
                    log.warn("FileCache: Negative pin count" + toString());
                    verify(false);
                }
                verify(buffer != null);
                pinCount--;
                if (pinCount == 0) {
                    buffer = null;
                }
            }
        }

        boolean matches(FileToken token, long offset)
        {
            return this.token == token && this.offset == offset;
        }

        // Age is function of access time and access count and retentionWeight.
        public long age(long now)
        {
            long age = (now - accessTime) / (accessCount + 1);
            return retentionWeight == 100 ? age : age * 100 / retentionWeight;
        }

        public void decay()
        {
            // This is called without synchronization. Do not write negative values.
            int count = accessCount;
            if (count > 1) {
                accessCount = count - 1;
            }
        }

        public boolean removeFromBucket()
        {
            BufferReference reference = softBuffer;
            if (reference == null) {
                return false;
            }
            return removeFromBucket(reference.getBucket(), false, reference, false);
        }

        public boolean removeFromBucket(int bucketIndex, boolean force, BufferReference reference, boolean mustFind)
        {
            List<Entry> bucket = entryHashTable[bucketIndex];
            synchronized (bucket) {
                if (!force && softBuffer != reference) {
                    return false;
                }
                if (!force && (pinCount > 0 || loadingFuture != null)) {
                    return false;
                }
                for (int i = 0; i < bucket.size(); i++) {
                    if (bucket.get(i) == this) {
                        bucket.remove(i);
                        if (isPrefetch) {
                            numWastedPrefetch++;
                            wastedPrefetchBytes += dataSize;
                            isPrefetch = false;
                        }
                        return true;
                    }
                }
                if (mustFind) {
                    verify(false, "Attempting to remove an entry that is not in its bucket");
                }
            }
            return false;
        }

        public void replaceInBucket(Entry other, int bucketIndex)
        {
            List<Entry> bucket = entryHashTable[bucketIndex];
            other.token = token;
            other.offset = offset;
            other.dataSize = dataSize;
            checkState(isTemporary);
            checkState(loadingFuture != null);
            other.loadingFuture = loadingFuture;
            other.loadingThread = loadingThread;
            other.isPrefetch = isPrefetch;
            other.listener = listener;
            other.retentionWeight = retentionWeight;
            if (other.softBuffer.getBucket() != bucketIndex) {
                verify(false, "Inserting entry with bad bucket index " + other.softBuffer.getBucket() + " into " + bucketIndex + " " + other.toString());
            }
            synchronized (bucket) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (bucket.get(i) == this) {
                        bucket.set(i, other);
                        return;
                    }
                }
            }
            verify(false, "Temp entry was not found in bucket");
        }

        public void ensureBuffer(int size, int bucketIndex)
        {
            BufferReference reference = softBuffer;
            if (reference != null) {
                buffer = reference.get();
                if (buffer != null && buffer.length >= size) {
                    softBuffer = new BufferReference(buffer, gcdBuffers, bucketIndex);
                    return;
                }
            }
            if (reference == null) {
                buffer = newBuffer(size);
                softBuffer = new BufferReference(buffer, gcdBuffers, bucketIndex);
            }
            else {
                buffer = softBuffer.get();
                if (buffer == null) {
                    numGcdBuffers++;
                    numGcdBytes += bufferSize;
                    totalSize.addAndGet(-bufferSize);
                    buffer = newBuffer(size);
                }
                softBuffer = new BufferReference(buffer, gcdBuffers, bucketIndex);
            }
            bufferSize = buffer.length;
            totalSize.addAndGet(bufferSize);
        }

        public void loadDone()
        {
            // Remove the future from the entry before marking it done. otherwise a waiting thread may see the same future on its next try and keep looping until this thread sets loadingFuture to null. Idempotent.
            SettableFuture<Boolean> future = loadingFuture;
            loadingFuture = null;
            if (future != null) {
                loadingThread = null;
                future.set(true);
            }
        }

        public void removeAndThrow(Exception e, int bucketIndex)
                throws IOException
        {
            log.warn("FileCache: Removing bad entry " + toString() + " caused by " + e.toString());
            removeFromBucket(bucketIndex, true, null, true);
            loadDone();
            throw new IOException(e);
        }

        @Override
        public String toString()
        {
            BufferReference reference = softBuffer;
            byte[] referencedBuffer = reference != null ? reference.get() : null;
            return toStringHelper(this)
                .addValue(isTemporary ? "Temporary" : "")
                .addValue(token)
                .add("offset", offset)
                .add("size", dataSize)
                .add("pins ", pinCount)
                .add("buffer", referencedBuffer != null ? "byte[" + referencedBuffer.length + "]" : "null")
                .toString();
        }
    }

    public static Entry get(OrcDataSource dataSource, long offset, int size)
            throws IOException
    {
        numGets++;
        return getInternal(dataSource, offset, size, false, System.nanoTime(), null, 100);
    }

    public static Entry get(OrcDataSource dataSource, long offset, int size, Listener listener, int retentionWeight)
            throws IOException
    {
        numGets++;
        return getInternal(dataSource, offset, size, false, System.nanoTime(), listener, retentionWeight);
    }

    private static Entry getInternal(OrcDataSource dataSource, long offset, int size, boolean isPrefetch, long now, Listener listener, int retentionWeight)
            throws IOException
    {
        FileToken token = dataSource.getToken();
        long hash = hashMix(token.hashCode(), offset);
        int bucketIndex = (int) hash & (entryHashTable.length - 1);
        List<Entry> list = entryHashTable[bucketIndex];
        if (list == null) {
            synchronized (FileCache.class) {
                list = entryHashTable[bucketIndex];
                if (offset == 2711273783L) {
                    System.out.println("bing");
                }
                if (list == null) {
                    list = new ArrayList();
                    entryHashTable[bucketIndex] = list;
                }
            }
        }
        int retryCount = 0;
        while (true) {
            trimGcd();
            SettableFuture futureToWait = null;
            Entry entryToWait = null;
            Entry entryToLoad = null;
            Entry hitEntry = null;
            synchronized (list) {
                for (Entry entry : list) {
                    if (entry.softBuffer == null) {
                        verify(entry.loadingFuture != null);
                    }
                    else if (entry.softBuffer.getBucket() != bucketIndex) {
                        verify(false, "Entry has bucket " + entry.softBuffer.getBucket() + " while in bucket " + bucketIndex + " " + entry.toString());
                    }
                    if (entry.matches(token, offset)) {
                        futureToWait = entry.loadingFuture;
                        if (futureToWait != null && isPrefetch) {
                            numLatePrefetch++;
                            return null;
                        }
                        if (isPrefetch && entry.softBuffer.get() != null) {
                            // The entry is in, set access time to avoid eviction but do not pin.
                            entry.accessTime = now;
                            return null;
                        }
                        if (futureToWait != null) {
                            numConcurrentMiss++;
                            entryToWait = entry;
                            break;
                        }
                        entry.buffer = entry.softBuffer.get();
                        if (entry.buffer == null) {
                            // The buffer was GC'd. This will come up in
                            // the reference queue but this will be a
                            // no-op since the bucket will not contain an
                            // entry with the equal BufferReference, thus
                            // the entry will not be removed.
                            entry.softBuffer = null;
                            totalSize.addAndGet(-entry.bufferSize);
                            numGcdBuffers++;
                            numGcdBytes += entry.bufferSize;
                            entryToLoad = entry;
                            entry.pinCount++;
                            entry.loadingFuture = SettableFuture.create();
                            entry.loadingThread = currentThread();
                            break;
                        }
                        if (entry.dataSize < size) {
                            entry.loadingFuture = SettableFuture.create();
                            entry.loadingThread = currentThread();
                            entry.pinCount++;
                            entry.isPrefetch = isPrefetch;
                            entryToLoad = entry;
                            break;
                        }

                        entry.pinCount++;
                        entry.accessTime = now;
                        // Enforce a range so as not to wrap around.
                        entry.accessCount = Math.min(entry.accessCount + 1, 10000);
                        if (!entry.isPrefetch) {
                            numHits++;
                            numHitBytes += entry.dataSize;
                        }
                        hitEntry = entry;
                        break;
                    }
                }
                if (entryToLoad == null && futureToWait == null && hitEntry == null) {
                    // There was a miss. While synchronized on the bucket, add an entry in loading state.
                    entryToLoad = Entry.createTemporary(token, offset, size, isPrefetch);
                    entryToLoad.listener = listener;
                    entryToLoad.retentionWeight = retentionWeight;
                    entryToLoad.accessTime = now;
                    list.add(entryToLoad);
                }
            }
            // Not synchronized on the bucket. If somebody else already loading, wait.
            if (futureToWait != null) {
                try {
                    long startWait = System.nanoTime();
                    futureToWait.get(20000, MILLISECONDS);
                    now = System.nanoTime();
                    readWaitMicros += (now - startWait) / 1000;
                }
                catch (TimeoutException e) {
                    log.warn("FileCache: Exceeded 20s waiting for other thread to load " + entryToWait.toString());
                    retryCount++;
                    if (retryCount > 2) {
                        // The entry stays in loading state. The
                        // loading thread is presumed to have errored
                        // out without removing the entry from the
                        // cache. This is an inconsistency that we
                        // cleanup here, otherwise the whole process
                        // will hang every time hitting the unfinished
                        // entry.
                        log.warn("FileCache: Entry in loading state for over 1 minute. Removing the entry from its bucket" + entryToWait.toString());
                        entryToWait.removeFromBucket(bucketIndex, true, null, false);
                    }
                    continue;
                }
                catch (Exception e) {
                    throw new IOException("Error in read signalled on other thread" + e.toString());
                }
                // The future was completed, the entry should be in.
                continue;
            }
            if (hitEntry != null) {
                boolean wasPrefetch = hitEntry.isPrefetch;
                hitEntry.isPrefetch = false;
                if (!wasPrefetch && hitEntry.listener != null) {
                    hitEntry.listener.hit(hitEntry);
                }
                try {
                    verify(hitEntry.pinCount > 0);
                    verify(hitEntry.buffer != null);
                    verify(hitEntry.buffer.length >= size);
                }
                catch (Exception e) {
                    hitEntry.removeAndThrow(e, bucketIndex);
                }
                return hitEntry;
            }
            Entry result = load(entryToLoad, size, dataSource, isPrefetch, bucketIndex, now);
            if (isPrefetch) {
                verify(result == null);
            }
            else {
                try {
                    verify(result.loadingFuture == null && (result.isTemporary || result.pinCount > 0));
                    verify(result.buffer.length >= size);
                }
                catch (Exception e) {
                    result.removeAndThrow(e, bucketIndex);
                }
                return result;
            }
        }
    }

    private static long hashMix(long h1, long h2)
    {
        return h1 ^ (h2 * M);
    }

    private static Entry load(Entry entry, int size, OrcDataSource dataSource, boolean isPrefetch, int bucketIndex, long now)
            throws IOException
    {
        checkState(entry.loadingFuture != null);
        Entry tempEntry = null;
        try {
            if (entry.isTemporary) {
                tempEntry = entry;
                Entry permanentEntry = getPermanentEntry(entry, size, bucketIndex, now);
                if (permanentEntry != null) {
                    verify(permanentEntry.loadingFuture != null && permanentEntry.pinCount == 1);
                    entry = permanentEntry;
                }
                else {
                    // If this is not a prefetch, this must succeed. Remove the temporary entry from the hash table and give it a buffer not owned by the cache.
                    entry.removeFromBucket(bucketIndex, true, null, true);
                    if (isPrefetch) {
                        entry.loadDone();
                        return null;
                    }
                    entry.buffer = newBuffer(size);
                }
                long startRead = 0;
                if (!isPrefetch) {
                    startRead = System.nanoTime();
                }
                dataSource.readFully(entry.offset, entry.buffer, 0, size);
                if (isPrefetch) {
                    numPrefetchRead++;
                    prefetchReadBytes += size;
                }
                else {
                    numOperatorRead++;
                    operatorReadMicros += (System.nanoTime() - startRead) / 1000;
                    operatorReadBytes += size;
                }
            }
            else {
                // If an entry is requested with a greater size than last request, there may be references to the buffer. Make a new buffer and replace only after loading.
                BufferReference reference = entry.softBuffer;
                byte[] oldBuffer = reference != null ? reference.get() : null;
                byte[] newBuffer;
                if (oldBuffer == null || oldBuffer.length < size) {
                    newBuffer = newBuffer(size);
                    totalSize.addAndGet(newBuffer.length - (oldBuffer != null ? oldBuffer.length : 0));
                }
                else {
                    newBuffer = oldBuffer;
                }
                long startRead = 0;
                if (!isPrefetch) {
                    startRead = System.nanoTime();
                }
                dataSource.readFully(entry.offset, newBuffer, 0, size);
                if (isPrefetch) {
                    numPrefetchRead++;
                    prefetchReadBytes += size;
                }
                else {
                    numOperatorRead++;
                    operatorReadMicros += (System.nanoTime() - startRead) / 1000;
                    operatorReadBytes += size;
                }
                entry.buffer = newBuffer;
                entry.bufferSize = newBuffer.length;
                entry.softBuffer = new BufferReference(newBuffer, gcdBuffers, bucketIndex);
                entry.dataSize = size;
            }
            entry.accessCount = 0;
            entry.accessTime = System.nanoTime();
            if (!entry.isTemporary && tempEntry != null) {
                int count = entry.pinCount;
                if (count != 1) {
                    log.warn("FileCache: pin count after load must always be 1: " + count + " seen, " + entry.toString());
                    verify(false, "A newly acquired permanent entry has pinCount != 1: " + entry.toString());
                }
            }
            entry.loadDone();
            if (entry.listener != null) {
                entry.listener.loaded(entry);
            }
            if (isPrefetch) {
                entry.close();
                return null;
            }
            verify(entry.loadingFuture == null);
            return entry;
        }
        catch (Exception e) {
            log.warn("FileCache: Error loading " + entry.toString() + ": " + e.toString());
            entry.close();
            entry.removeFromBucket();
            entry.loadDone();
            throw e;
        }
    }

    private static byte[] newBuffer(int size)
    {
        byte[] buffer = byteArrayPool.allocate(size);
        bytesAllocated += buffer.length;
        return buffer;
    }

    private static void trimGcd()
    {
        while (true) {
            Reference<? extends byte[]> reference = gcdBuffers.poll();
            if (reference == null) {
                return;
            }
            BufferReference bufferReference = (BufferReference) reference;
            List<Entry> bucket = entryHashTable[bufferReference.getBucket()];
            synchronized (bucket) {
                for (int i = 0; i < bucket.size(); i++) {
                    Entry entry = bucket.get(i);
                    if (entry.softBuffer == bufferReference) {
                        totalSize.addAndGet(-entry.bufferSize);
                        numGcdBuffers++;
                        numGcdBytes += entry.bufferSize;
                        bucket.remove(i);
                        checkState(entry.buffer == null);
                        entry.softBuffer = null;
                        entry.token = null;
                    }
                }
            }
        }
    }

    private static Entry getPermanentEntry(Entry tempEntry, int size, int newBucket, long now)
    {
        // Finds a suitably old entry to reuse. Periodically updates stats. If no entry with the size is found, removes an equivalent amount of different size entries and makes a new buffer of the requested size. Does a dirty read of the pin counts and scores. When finding a suitable entry, synchronizes on its bucket and removes it, checking that the score and pin count are still as needed. If size is at max and nothing is reusable, returns null.
        size = byteArrayPool.getStandardSize(size);
        int numLoops = 0;
        boolean allEntriesExist = numEntries >= MAX_ENTRIES;
        long bestAge = Long.MIN_VALUE;
        long bestAgeWithSize = Long.MIN_VALUE;
        Entry best = null;
        Entry bestWithSize = null;
        Entry empty = null;
        while (true) {
            int end = numEntries;
            if (numGets - numGetsForStats > end / 8) {
                numGetsForStats = numGets;
                if (useThreshold) {
                    now = System.nanoTime();
                    updateEvictionThreshold(now);
                }
                else {
                    evictionThreshold = Long.MIN_VALUE;
                }
            }
            if (numLoops > end * 2) {
                numAllInUse++;
                return null;
            }
            if (useThreshold && numLoops >= end && numLoops < end + 20) {
                now = System.nanoTime();
                updateEvictionThreshold(now);
            }
            long threshold = evictionThreshold;
            if (threshold > Long.MIN_VALUE && numLoops > 40) {
                threshold = (long) (threshold / Math.max(1.1, (numLoops / 300.0)));
            }
            numLoops += 20;
            int startIndex = (clockHand & 0xffffff) % end;
            clockHand += 20;
            boolean atCapacity = totalSize.get() + size > targetSize || freeMemory() < 200 << (1 << 20);
            if (initializing && atCapacity) {
                initializing = false;
            }
            for (int i = 0; i < 20; i++, startIndex = startIndex >= end - 1 ? 0 : startIndex + 1) {
                Entry entry = entries[startIndex];
                if (entry.pinCount == 0 && entry.loadingFuture == null) {
                    BufferReference reference = entry.softBuffer;
                    if (reference == null || reference.get() == null) {
                        empty = entry;
                        if (initializing || (!atCapacity && numLoops > end / 2)) {
                            break;
                        }
                        continue;
                    }
                    long age = entry.age(now);
                    if (numLoops < end && age < evictionThreshold + (now - statsTime)) {
                        continue;
                    }
                    if (entry.bufferSize == size && age > bestAgeWithSize) {
                        bestWithSize = entry;
                        bestAgeWithSize = age;
                        continue;
                    }
                    if (best != null && entry.bufferSize > best.bufferSize || age > bestAge) {
                        bestAge = age;
                        best = entry;
                    }
                }
            }
            // If all memory used, free the oldest that does not have the size and recycle the oldest that had the size.
            if (atCapacity || (allEntriesExist && empty == null) ||
                    (!initializing && numLoops < 100)) {
                boolean wasPrefetch = bestWithSize != null && bestWithSize.isPrefetch;
                if (bestWithSize != null && bestWithSize.removeFromBucket()) {
                    sumEvictAge += now - bestWithSize.accessTime;
                    numEvicts++;
                    if (bestWithSize.listener != null) {
                        bestWithSize.listener.evicted(bestWithSize, now, wasPrefetch);
                    }
                    checkState(bestWithSize.pinCount == 0);
                    bestWithSize.pinCount = 1;
                    bestWithSize.ensureBuffer(size, newBucket);
                    tempEntry.replaceInBucket(bestWithSize, newBucket);
                    return bestWithSize;
                }
                wasPrefetch = best != null && best.isPrefetch;
                if (atCapacity && numLoops > end && best != null && best.removeFromBucket()) {
                    // This is safe, only one thread can successfully remove.
                    sumEvictAge += now - best.accessTime;
                    numEvicts++;
                    if (best.listener != null) {
                        best.listener.evicted(best, now, wasPrefetch);
                    }
                    BufferReference reference = best.softBuffer;
                    byte[] buffer = reference == null ? null : reference.get();
                    if (buffer == null || buffer.length != best.bufferSize) {
                        log.warn("FileCache: Bad decrement of totalSize: " + best.toString());
                    }
                    totalSize.addAndGet(-best.bufferSize);
                    best.softBuffer = null;
                    best.buffer = null;
                }
            }
            else {
                if (numLoops > 30 && empty == null) {
                    empty = findOrMakeEmpty();
                    if (empty != null) {
                        // This is a new guaranteed unused empty.
                        empty.pinCount = 1;
                        empty.ensureBuffer(size, newBucket);
                        tempEntry.replaceInBucket(empty, newBucket);
                        return empty;
                    }
                }
                if (empty != null) {
                    // Synchronize Cache-wide to see that empty is still empty.
                    synchronized (FileCache.class) {
                        if (empty.softBuffer != null || empty.pinCount != 0) {
                            continue;
                        }
                        empty.pinCount = 1;
                    }
                    empty.ensureBuffer(size, newBucket);
                    tempEntry.replaceInBucket(empty, newBucket);
                    return empty;
                }
            }
        }
    }

    private static long freeMemory()
    {
        return Runtime.getRuntime().freeMemory();
    }

    private static void updateEvictionThreshold(long now)
    {
        // Sample a few ages  and return bottom 20 percentile.
        int end = numEntries;
        int numSamples = Math.min(end / 20, 500);
        int step = end / numSamples;
        long[] samples = new long[numSamples];
        int sampleCount = 0;
        int numLoops = 0;
        while (true) {
            int startIndex = (statsClockHand++ & 0xffffff) % end;
            for (int i = 0; i < numSamples && sampleCount < numSamples; i++, startIndex = (startIndex + step >= end) ? startIndex + step - end : startIndex + step) {
                Entry entry = entries[startIndex];
                BufferReference reference = entry.softBuffer;
                if (reference != null && reference.get() != null) {
                    samples[sampleCount++] = entry.age(now);
                }
            }
            if (sampleCount >= numSamples || ++numLoops > 10) {
                break;
            }
        }
        Arrays.sort(samples, 0, sampleCount);
        statsTime = now;
        evictionThreshold = sampleCount == 0 ? Long.MIN_VALUE : samples[(sampleCount / 5) * 4];
    }

    private static Entry findOrMakeEmpty()
    {
        if (numEntries >= MAX_ENTRIES) {
            return null;
        }
        Entry newEntry = new Entry(0);
        synchronized (FileCache.class) {
            if (numEntries == MAX_ENTRIES) {
                return null;
            }
            entries[numEntries] = newEntry;
            // All below numEntries must appear filled for dirty readers.
            numEntries++;
            return entries[numEntries - 1];
        }
    }

    private static ExecutorService getExecutor()
    {
        if (prefetchExecutor != null) {
            return prefetchExecutor;
        }
        synchronized (FileCache.class) {
            if (prefetchExecutor != null) {
                return prefetchExecutor;
            }
            prefetchExecutor = Executors.newFixedThreadPool(60);
        }
        return prefetchExecutor;
    }

    public static void asyncPrefetch(OrcDataSource dataSource, long offset, int size, Listener listener, int retentionWeight)
    {
        if (prefetchSize.get() > MAX_PREFETCH_SIZE) {
            return;
        }
        prefetchSize.addAndGet(size);
        getExecutor().submit(() -> {
            prefetchSize.addAndGet(-size);
            String name = currentThread().getName();
            try {
                numPrefetch++;
                prefetchBytes += size;
                currentThread().setName("prefetch");
                long startTime = System.nanoTime();
                FileCache.getInternal(dataSource, offset, size, true, startTime, listener, retentionWeight);
                prefetchMicros += (System.nanoTime() - startTime) / 1000;
            }
            catch (Exception e) {
                log.warn("FileCache: Error in prefetch " + e.toString());
            }
            finally {
                currentThread().setName(name);
            }
        });
    }

    public static void registerStats(FileCacheStats stats)
    {
        statsBean = stats;
    }

    public static class FileCacheStats
    {
        @Managed
        public long getGets()
        {
            return numGets;
        }

        @Managed
        public long getHits()
        {
            return numHits;
        }

        @Managed
        public long getHitBytes()
        {
            return numHitBytes;
        }

        @Managed
        public long getPrefetch()
        {
            return numPrefetch;
        }

        @Managed
        public long getGcdBuffers()
        {
            return numGcdBuffers;
        }

        @Managed
        public long getGcdBytes()
        {
            return numGcdBytes;
        }

        @Managed
        public long getPrefetchBytes()
        {
            return prefetchBytes;
        }

        @Managed
        public long getLatePrefetch()
        {
            return numLatePrefetch;
        }

        @Managed
        public long getWastedPrefetch()
        {
            return numWastedPrefetch;
        }

        @Managed
        public long getWastedPrefetchBytes()
        {
            return wastedPrefetchBytes;
        }

        @Managed
        public long getConcurrentMiss()
        {
            return numConcurrentMiss;
        }

        @Managed
        public long getAllInUse()
        {
            return numAllInUse;
        }

        @Managed
        public long getTotalSize()
        {
            return totalSize.get();
        }

        @Managed
        public long getNumEntries()
        {
            return numEntries;
        }

        @Managed
        public long getPinned()
        {
            int count = 0;
            int end = numEntries;
            for (int i = 0; i < end; i++) {
                if (entries[i].pinCount != 0) {
                    count++;
                }
            }
            return count;
        }

        @Managed
        public long getLoading()
        {
            int count = 0;
            int end = numEntries;
            for (int i = 0; i < end; i++) {
                if (entries[i].loadingFuture != null) {
                    count++;
                }
            }
            return count;
        }

        @Managed
        public long getAllocatedSize()
        {
            long size = 0;
            int end = numEntries;
            for (int i = 0; i < end; i++) {
                BufferReference reference = entries[i].softBuffer;
                byte[] buffer = reference == null ? null : reference.get();
                if (buffer != null) {
                    size += buffer.length;
                }
            }
            return size;
        }

        @Managed
        public long getUsedSize()
        {
            long size = 0;
            int end = numEntries;
            for (int i = 0; i < end; i++) {
                BufferReference reference = entries[i].softBuffer;
                byte[] buffer = reference == null ? null : reference.get();
                if (buffer != null) {
                    size += entries[i].dataSize;
                }
            }
            return size;
        }

        @Managed
        public long getPendingPrefetch()
        {
            return prefetchSize.get();
        }

        @Managed
        public long getAverageLifetimeMillis()
        {
            return sumEvictAge / 1000000 / (numEvicts | 1);
        }

        @Managed
        public long getPrefetchMicros()
        {
            return prefetchMicros;
        }

        @Managed
        public long getNumPrefetchRead()
        {
            return numPrefetchRead;
        }

        @Managed
        public long getPrefetchReadBytes()
        {
            return prefetchReadBytes;
        }

        @Managed
        public long getReadWaitMicros()
        {
            return readWaitMicros;
        }

        @Managed
        public long getNumOperatorRead()
        {
            return numOperatorRead;
        }

        @Managed
        public long getOperatorReadBytes()
        {
            return operatorReadBytes;
        }

        @Managed
        public long getOperatorReadMicros()
        {
            return operatorReadMicros;
        }

        @Managed
        public long getBytesAllocated()
        {
            return bytesAllocated;
        }

        @Managed
        public String getSizeReport()
        {
            int numSizes = byteArrayPool.getStandardSizes().length;
            long[] ages = new long[numSizes];
            long[] unhitPrefetchBytes = new long[numSizes];
            int[] counts = new int[numSizes];
            int end = numEntries;
            long now = System.nanoTime();
            for (int i = 0; i < end; i++) {
                Entry entry = entries[i];
                BufferReference reference = entry.softBuffer;
                byte[] buffer = reference == null ? null : reference.get();
                if (buffer != null) {
                    int sizeIndex = byteArrayPool.getSizeIndex(buffer.length);
                    ages[sizeIndex] += entry.age(now);
                    counts[sizeIndex]++;
                    if (entry.isPrefetch) {
                        unhitPrefetchBytes[sizeIndex] += entry.bufferSize;
                    }
                }
            }
            String result = "Sizes:\n";
            for (int i = 0; i < numSizes; i++) {
                if (counts[i] > 0) {
                    int size = byteArrayPool.getStandardSizes()[i];
                    long percent = ((long) size * counts[i] * 100) / totalSize.get();
                    long pendingSize = unhitPrefetchBytes[i] / 1024;
                    result = result + (size / 1024) + "K: " + percent + "% " + counts[i] + " Age ms: " + ages[i] / counts[i] / 1000000 + (pendingSize > 0 ? " pending use " + pendingSize + "K" : "") + "\n";
                }
            }
            return result;
        }

        @Managed
        public String getHitReport()
        {
            Set<Listener> listeners = new HashSet();
            int end = numEntries;
            for (int i = 0; i < end; i++) {
                Entry entry = entries[i];
                BufferReference reference = entry.softBuffer;
                byte[] buffer = reference == null ? null : reference.get();
                if (buffer != null) {
                    Listener listener = entry.listener;
                    if (listener != null) {
                        listeners.add(listener);
                    }
                }
            }
            Listener[] array = listeners.toArray(new Listener[listeners.size()]);
            Arrays.sort(array, (Listener a, Listener b) -> a.compare(b));
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < 100 && i < array.length; i++) {
                Listener listener = array[i];
                result.append(listener.toString());
                result.append("\n");
            }
            return result.toString();
        }
    }
}
