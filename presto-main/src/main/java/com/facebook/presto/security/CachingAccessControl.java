package com.facebook.presto.security;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.spi.security.AccessControlContext.CACHE_REFRESH_CONTEXT;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingAccessControl
        implements AccessControl
{
    private final AccessControl delegate;
    private final LoadingCache<CatalogAccess, Boolean> catalogAccessCache;

    @Inject
    public CachingAccessControl(AccessControl delegate, ExecutorService executor, AccessControlConfig accessControlConfig)
    {
        this(
                delegate,
                executor,
                accessControlConfig.getAccessControlCacheTtl(),
                accessControlConfig.getAccessControlRefreshTtl(),
                accessControlConfig.getAccessControlCacheMaximumSize());
    }

    public CachingAccessControl(AccessControl delegate, ExecutorService executor, Duration cacheTtl, Duration refreshInterval, long maximumSize)
    {
        this(
                delegate,
                executor,
                OptionalLong.of(cacheTtl.toMillis()),
                refreshInterval.toMillis() >= cacheTtl.toMillis() ? OptionalLong.empty() : OptionalLong.of(refreshInterval.toMillis()),
                maximumSize);
    }

    private CachingAccessControl(AccessControl delegate, ExecutorService executor, OptionalLong expiresAfterWriteMillis, OptionalLong refreshMills, long maximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");

        this.catalogAccessCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadCatalogAccess), executor));
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis, OptionalLong refreshMillis, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MILLISECONDS);
        }
        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), MILLISECONDS);
        }
        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    @Override
    public void checkCanSetUser(AccessControlContext accessControlContext, Optional<Principal> principal, String userName)
    {
        delegate.checkCanSetUser(accessControlContext, principal, userName);
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query)
    {
        delegate.checkQueryIntegrity(identity, context, query);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        return delegate.filterCatalogs(identity, context, catalogs);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
        get(catalogAccessCache, new CatalogAccess(identity, catalogName));
    }

    private boolean loadCatalogAccess(CatalogAccess catalogAccess)
    {
        try {
            delegate.checkCanAccessCatalog(catalogAccess.getIdentity(), CACHE_REFRESH_CONTEXT, catalogAccess.getCatalogName());
            return true;
        }
        catch (PrestoException e) {
            catalogAccessCache.invalidate(catalogAccess);
            throw e;
        }
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {

    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName)
    {

    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schemaName, String newSchemaName)
    {

    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {

    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        return null;
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {

    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, AccessControlContext context, CatalogSchemaName schema)
    {

    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return null;
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName)
    {

    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {

    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName viewName)
    {

    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {

    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {

    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, AccessControlContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {

    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {

    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName, String propertyName)
    {

    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {

    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, AccessControlContext context, String role, String catalogName)
    {

    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, AccessControlContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, AccessControlContext context, String role, String catalog)
    {

    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {

    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {

    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, AccessControlContext context, String catalogName)
    {

    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private class CatalogAccess
    {
        private final Identity identity;
        private final String catalogName;

        public CatalogAccess(Identity identity, String catalogName)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        public Identity getIdentity()
        {
            return identity;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogAccess that = (CatalogAccess) o;
            return identity.equals(that.identity) &&
                    catalogName.equals(that.catalogName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, catalogName);
        }
    }

    private static class SystemSessionPropertyAccess
    {
        private final Identity identity;
        private final String propertyName;

        public SystemSessionPropertyAccess(Identity identity, String propertyName)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.propertyName = requireNonNull(propertyName, "propertyName is null");
        }

        public Identity getIdentity()
        {
            return identity;
        }

        public String getPropertyName()
        {
            return propertyName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SystemSessionPropertyAccess that = (SystemSessionPropertyAccess) o;
            return Objects.equals(identity, that.identity) &&
                    Objects.equals(propertyName, that.propertyName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, propertyName);
        }
    }
}
