# Google Firestore Binding for YCSB

This binding allows YCSB to benchmark Google Cloud Firestore, a flexible, scalable NoSQL cloud database.

## Getting Started

### Prerequisites

1. **Google Cloud Project**: Create a project at [Google Cloud Console](https://console.cloud.google.com/)
2. **Enable Firestore API**: Enable the Cloud Firestore API for your project
3. **Authentication**: Set up authentication using one of these methods:
   - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key file
   - Use Application Default Credentials (ADC) if running on GCP

### Configuration

The following properties are required/optional:

| Property | Required | Description | Default |
|----------|----------|-------------|---------|
| `googlefirestore.project` | Yes | Google Cloud Project ID | - |
| `googlefirestore.database` | No | Firestore database name | `(default)` |
| `googlefirestore.endpoint` | No | Custom endpoint (useful for testing) | - |
| `debug` | No | Enable debug logging | `false` |

### Example Configuration

Create a file `firestore.properties`:

```properties
# Required: Your Google Cloud Project ID
googlefirestore.project=my-gcp-project

# Optional: Database name (defaults to "(default)")
# googlefirestore.database=(default)

# Optional: Custom endpoint (e.g., for emulator)
# googlefirestore.endpoint=localhost:8080

# Optional: Enable debug output
debug=false
```

## Building

Build the Firestore binding:

```bash
# Windows
build-firebase.bat

# Linux/Mac
mvn clean install -pl googlefirestore -am -DskipTests -Dcheckstyle.skip=true
```

## Running Benchmarks

### 1. Load Phase

Load data into Firestore:

```bash
./bin/ycsb load googlefirestore -P workloads/workloada \
  -P firestore.properties \
  -p recordcount=10000
```

### 2. Run Phase

Run the benchmark:

```bash
./bin/ycsb run googlefirestore -P workloads/workloada \
  -P firestore.properties \
  -p operationcount=10000
```

## Using with Firestore Emulator

For local testing without incurring costs, use the [Firestore Emulator](https://firebase.google.com/docs/emulator-suite/connect_firestore):

1. **Start the emulator**:
   ```bash
   gcloud emulators firestore start --host-port=localhost:8080
   ```

2. **Set environment variable**:
   ```bash
   export FIRESTORE_EMULATOR_HOST=localhost:8080
   ```

3. **Run YCSB**:
   ```bash
   ./bin/ycsb load googlefirestore -P workloads/workloada \
     -P firestore.properties \
     -p recordcount=1000
   ```

The client will automatically detect the `FIRESTORE_EMULATOR_HOST` environment variable.

## Workload Mapping

YCSB operations map to Firestore as follows:

| YCSB Operation | Firestore Operation |
|----------------|---------------------|
| `read` | `document.get()` |
| `scan` | `collection.orderBy().startAfter().limit()` |
| `update` | `document.update()` |
| `insert` | `document.set()` |
| `delete` | `document.delete()` |

### Data Model

- **Table** → Firestore Collection
- **Key** → Document ID
- **Fields** → Document fields (stored as String values)

Example document structure:
```json
{
  "field0": "value0",
  "field1": "value1",
  "field2": "value2",
  ...
}
```

## Performance Considerations

1. **Indexes**: Firestore automatically indexes all fields. For scan operations, composite indexes may be required for production use.

2. **Costs**: Be aware of [Firestore pricing](https://cloud.google.com/firestore/pricing):
   - Document reads/writes
   - Storage costs
   - Network egress

3. **Throughput**: Firestore has limits on writes per second per document. For high-throughput workloads, consider:
   - Using different document keys to avoid hot spots
   - Implementing batch writes (future enhancement)

4. **Latency**: Network latency can impact results. Consider running benchmarks from:
   - A VM in the same region as your Firestore instance
   - Using regional Firestore instances close to your application

## Troubleshooting

### Authentication Errors

**Error**: `The Application Default Credentials are not available`

**Solution**: Set up authentication:
```bash
# Option 1: Service Account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Option 2: gcloud login (for development)
gcloud auth application-default login
```

### Permission Errors

**Error**: `PERMISSION_DENIED: Missing or insufficient permissions`

**Solution**: Ensure your service account has the required roles:
- `Cloud Datastore User` or
- `Cloud Datastore Owner`

### Maven Build Issues

See [BUILDING-FIREBASE.md](../BUILDING-FIREBASE.md) for detailed build instructions and troubleshooting.

## Example Workloads

### Small Test Run (Emulator)
```bash
# Start emulator
export FIRESTORE_EMULATOR_HOST=localhost:8080

# Load 1000 records
./bin/ycsb load googlefirestore -P workloads/workloada \
  -p googlefirestore.project=test-project \
  -p recordcount=1000 \
  -p threadcount=10

# Run workload A (50% read, 50% update)
./bin/ycsb run googlefirestore -P workloads/workloada \
  -p googlefirestore.project=test-project \
  -p operationcount=5000 \
  -p threadcount=10
```

### Production Benchmark
```bash
# Load 1 million records
./bin/ycsb load googlefirestore -P workloads/workloada \
  -P firestore.properties \
  -p recordcount=1000000 \
  -p threadcount=50

# Run workload B (95% read, 5% update)
./bin/ycsb run googlefirestore -P workloads/workloadb \
  -P firestore.properties \
  -p operationcount=1000000 \
  -p threadcount=50
```

## Implementation Details

### Initialization

The client uses a singleton pattern with reference counting:
- First thread initializes the global `Firestore` client
- Subsequent threads reuse the same client
- Last thread to cleanup closes the connection

### Thread Safety

The Firestore client is thread-safe and can be shared across multiple threads.

### Connection Pooling

The Firestore client automatically manages connection pooling and retries.

## Future Enhancements

Potential improvements for this binding:

1. **Batch Operations**: Implement batch writes for insert/update operations
2. **Transactions**: Support for transactional operations
3. **Async Operations**: Use async APIs for better throughput
4. **Metrics**: Detailed metrics collection (latency percentiles, error rates)
5. **Subcollections**: Support for hierarchical data models
6. **Query Filters**: More complex query patterns

## References

- [Cloud Firestore Documentation](https://cloud.google.com/firestore/docs)
- [Firestore Java Client Library](https://cloud.google.com/java/docs/reference/google-cloud-firestore/latest/overview)
- [YCSB Core Properties](https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties)
- [YCSB Workload Documentation](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads)
