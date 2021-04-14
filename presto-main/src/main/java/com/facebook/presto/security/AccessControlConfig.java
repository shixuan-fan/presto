package com.facebook.presto.security;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AccessControlConfig
{
    private Duration accessControlCacheTtl = new Duration(0, SECONDS);
    private Duration accessControlRefreshTtl = new Duration(0, SECONDS);
    private long accessControlCacheMaximumSize = 10000;
    private int maxAccessControlRefreshThreads = 100;

    @NotNull
    public Duration getAccessControlCacheTtl()
    {
        return accessControlCacheTtl;
    }

    @MinDuration("0ms")
    @Config("security.access-control-cache-ttl")
    public AccessControlConfig setAccessControlCacheTtl(Duration accessControlCacheTtl)
    {
        this.accessControlCacheTtl = accessControlCacheTtl;
        return this;
    }

    @NotNull
    public Duration getAccessControlRefreshTtl()
    {
        return accessControlRefreshTtl;
    }

    @MinDuration("1ms")
    @Config("security.access-control-refresh-interval")
    public AccessControlConfig setAccessControlRefreshTtl(Duration accessControlRefreshTtl)
    {
        this.accessControlRefreshTtl = accessControlRefreshTtl;
        return this;
    }

    public long getAccessControlCacheMaximumSize()
    {
        return accessControlCacheMaximumSize;
    }

    @Min(1)
    @Config("security.access-control-cache-maximum-size")
    public AccessControlConfig setAccessControlCacheMaximumSize(long accessControlCacheMaximumSize)
    {
        this.accessControlCacheMaximumSize = accessControlCacheMaximumSize;
        return this;
    }

    public int getMaxAccessControlRefreshThreads()
    {
        return maxAccessControlRefreshThreads;
    }

    @Min(1)
    @Config("security.access-control-refresh-max-threads")
    public AccessControlConfig setMaxAccessControlRefreshThreads(int maxAccessControlRefreshThreads)
    {
        this.maxAccessControlRefreshThreads = maxAccessControlRefreshThreads;
        return this;
    }
}
