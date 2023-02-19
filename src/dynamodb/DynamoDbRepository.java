package dynamodb;

import lombok.Builder;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Builder
@ApplicationScoped
public class DynamoDbRepositoryg {

  @Inject DynamoDbEnhancedClient dynamoDbEnhancedAsyncClient;
  private static final String TABLE_NAME = "table_name";
  private static final String DOMAIN_GSI = "domainIndex";
  private static final Logger LOG = Logger.getLogger(PropertyTenantRepository.class);

  public void put(final DynamoDbModel dynamoDbModel) {
    LOG.info(dynamoDbModel);
    getTable().putItem(dynamoDbModel);
  }

  public DynamoDbModel updateTenant(final DynamoDbModel dynamoDbModel) {
    LOG.info(dynamoDbModel);
    return getTable().updateItem(dynamoDbModel);
  }

  public DynamoDbModel deleteTenant(final String partitionKeyId, final String sortKeyId) {
    return getTable()
        .deleteItem(TenantDynamoDbModel.builder().id(partitionKeyId).domain(sortKeyId).build());
  }

  public DynamoDbModel getTenant(final String partitionKeyId, final String sortKeyId) {
    return getTable().getItem(DynamoDbModel.builder().id(partitionKeyId).domain(sortKeyId).build());
  }

  public SdkIterable<Page<DynamoDbModel>> listBasedOnGSI(final String gsiId) {
    return getTable()
        .index(DOMAIN_GSI)
        .query(
            QueryEnhancedRequest.builder()
                .queryConditional(
                    QueryConditional.keyEqualTo(Key.builder().partitionValue(gsiId).build()))
                .build());
  }

  private DynamoDbTable<TenantDynamoDbModel> getTable() {
    return dynamoDbEnhancedAsyncClient.table(
        TABLE_NAME, TableSchema.fromBean(TenantDynamoDbModel.class));
  }
}
