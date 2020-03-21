package com.facebook.presto.security;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingAccessControl
        implements AccessControl
{
    private final AccessControl delegate;
    private final LoadingCache<CatalogAccess, Boolean> catalogAccessCache;
    private final LoadingCache<SystemSessionPropertyAccess, Boolean> systemSessionPropertyAccessCache;
    private final Cache<Identity, Set<CatalogSessionProperty>> catalogSessionPropertyAccessCache;

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

    private CachingAccessControl(AccessControl delegate, ExecutorService executor,  OptionalLong expiresAfterWriteMillis, OptionalLong refreshMills, long maximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(executor, "executor is null");

        this.catalogAccessCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadCatalogAccess), executor));
        this.systemSessionPropertyAccessCache = newCacheBuilder(expiresAfterWriteMillis, refreshMills, maximumSize)
                .build(asyncReloading(CacheLoader.from(this::loadSystemSessionPropertyAccess), executor));
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        delegate.checkCanSetUser(principal, userName);
    }

    @Override
    public void checkQueryIntegrity(Identity identity, String query)
    {
        delegate.checkQueryIntegrity(identity, query);
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return delegate.filterCatalogs(identity, catalogs);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        try {
            Set<String> allowedCatalogNames = catalogAccessCache.get(identity, ConcurrentHashMap::newKeySet);
            if (!allowedCatalogNames.contains(catalogName)) {
                delegate.checkCanAccessCatalog(identity, catalogName);
                allowedCatalogNames.add(catalogName);
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to create concurrent hash set for catalog access cache", e.getCause());
        }
    }

    private boolean loadCatalogAccess(CatalogAccess catalogAccess)
    {
        delegate.checkCanAccessCatalog(catalogAccess.getIdentity(), catalogAccess.getCatalogName());
        return true;
    }

    @Override
    public void checkCanCreateSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        delegate.checkCanCreateSchema(transactionId, identity, schemaName);
    }

    @Override
    public void checkCanDropSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName)
    {
        delegate.checkCanDropSchema(transactionId, identity, schemaName);
    }

    @Override
    public void checkCanRenameSchema(TransactionId transactionId, Identity identity, CatalogSchemaName schemaName, String newSchemaName)
    {
        delegate.checkCanRenameSchema(transactionId, identity, schemaName, newSchemaName);
    }

    @Override
    public void checkCanShowSchemas(TransactionId transactionId, Identity identity, String catalogName)
    {
        delegate.checkCanShowSchemas(transactionId, identity, catalogName);
    }

    @Override
    public Set<String> filterSchemas(TransactionId transactionId, Identity identity, String catalogName, Set<String> schemaNames)
    {
        return delegate.filterSchemas(transactionId, identity, catalogName, schemaNames);
    }

    @Override
    public void checkCanCreateTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanCreateTable(transactionId, identity, tableName);
    }

    @Override
    public void checkCanDropTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanDropTable(transactionId, identity, tableName);
    }

    @Override
    public void checkCanRenameTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        delegate.checkCanRenameTable(transactionId, identity, tableName, newTableName);
    }

    @Override
    public void checkCanShowTablesMetadata(TransactionId transactionId, Identity identity, CatalogSchemaName schema)
    {
        delegate.checkCanShowTablesMetadata(transactionId, identity, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(TransactionId transactionId, Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        return delegate.filterTables(transactionId, identity, catalogName, tableNames);
    }

    @Override
    public void checkCanAddColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanAddColumns(transactionId, identity, tableName);
    }

    @Override
    public void checkCanDropColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanDropColumn(transactionId, identity, tableName);
    }

    @Override
    public void checkCanRenameColumn(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanRenameColumn(transactionId, identity, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanInsertIntoTable(transactionId, identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        delegate.checkCanDeleteFromTable(transactionId, identity, tableName);
    }

    @Override
    public void checkCanCreateView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        delegate.checkCanCreateView(transactionId, identity, viewName);
    }

    @Override
    public void checkCanDropView(TransactionId transactionId, Identity identity, QualifiedObjectName viewName)
    {
        delegate.checkCanDropView(transactionId, identity, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate.checkCanCreateViewWithSelectFromColumns(transactionId, identity, tableName, columnNames);
    }

    @Override
    public void checkCanGrantTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        delegate.checkCanGrantTablePrivilege(transactionId, identity, privilege, tableName, grantee, withGrantOption);
    }

    @Override
    public void checkCanRevokeTablePrivilege(TransactionId transactionId, Identity identity, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        delegate.checkCanRevokeTablePrivilege(transactionId, identity, privilege, tableName, revokee, grantOptionFor);
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        try {
            Set<String> allowedSystemSessionProperties = systemSessionPropertyAccessCache.get(identity, ConcurrentHashMap::newKeySet);
            if (!allowedSystemSessionProperties.contains(propertyName)) {
                delegate.checkCanSetSystemSessionProperty(identity, propertyName);
                allowedSystemSessionProperties.add(propertyName);
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to create concurrent hash set for system session property cache", e.getCause());
        }
    }

    private boolean loadSystemSessionPropertyAccess(SystemSessionPropertyAccess systemSessionPropertyAccess)
    {
        delegate.checkCanSetSystemSessionProperty(systemSessionPropertyAccess.getIdentity(), systemSessionPropertyAccess.getPropertyName());
        return true;
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
        try {
            Set<CatalogSessionProperty> allowedCatalogSessionProperties = catalogSessionPropertyAccessCache.get(identity, ConcurrentHashMap::newKeySet);
            CatalogSessionProperty catalogSessionProperty = new CatalogSessionProperty(catalogName, propertyName);
            if (!allowedCatalogSessionProperties.contains(catalogSessionProperty)) {
                delegate.checkCanSetCatalogSessionProperty(transactionId, identity, catalogName, propertyName);
                allowedCatalogSessionProperties.add(catalogSessionProperty);
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to create concurrent hash set for catalog session property cache", e.getCause());
        }
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {

    }

    @Override
    public void checkCanCreateRole(TransactionId transactionId, Identity identity, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanDropRole(TransactionId transactionId, Identity identity, String role, String catalogName)
    {

    }

    @Override
    public void checkCanGrantRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanRevokeRoles(TransactionId transactionId, Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {

    }

    @Override
    public void checkCanSetRole(TransactionId requiredTransactionId, Identity identity, String role, String catalog)
    {

    }

    @Override
    public void checkCanShowRoles(TransactionId transactionId, Identity identity, String catalogName)
    {

    }

    @Override
    public void checkCanShowCurrentRoles(TransactionId transactionId, Identity identity, String catalogName)
    {

    }

    @Override
    public void checkCanShowRoleGrants(TransactionId transactionId, Identity identity, String catalogName)
    {

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

    private static class CatalogSessionPropertyAccess
    {
        private final Identity identity;
        private final String catalogName;
        private final String propertyName;

        public CatalogSessionProperty(String catalogName, String propertyName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.propertyName = requireNonNull(propertyName, "propertyName is null");
        }

        public String getCatalogName()
        {
            return catalogName;
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
            CatalogSessionProperty that = (CatalogSessionProperty) o;
            return Objects.equals(catalogName, that.catalogName) &&
                    Objects.equals(propertyName, that.propertyName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalogName, propertyName);
        }
    }
}
