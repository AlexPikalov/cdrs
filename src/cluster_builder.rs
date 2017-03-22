use authenticators::NoneAuthenticator;
use compression::Compression;




// private final List<InetAddress> addresses = new ArrayList<InetAddress>();
// private int port = ProtocolOptions.DEFAULT_PORT;
// private AuthInfoProvider authProvider = AuthInfoProvider.NONE;

// private LoadBalancingPolicy loadBalancingPolicy;
// private ReconnectionPolicy reconnectionPolicy;
// private RetryPolicy retryPolicy;

// private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
// private SSLOptions sslOptions = null;
// private boolean metricsEnabled = true;
// private boolean jmxEnabled = true;
// private final PoolingOptions poolingOptions = new PoolingOptions();
// private final SocketOptions socketOptions = new SocketOptions();

const DEFAULT_PORT : i32 = 9042;

#[derive(Debug)]
struct ClusterBuilder<'s> {
    addresses: Vec<&'s str>,
    port : i32,
    authenticator : NoneAuthenticator,
    
    
}