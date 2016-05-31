/**
 * 
 */
package com.daedafusion.discovery.etcd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.daedafusion.jetcd.EtcdClient;
import com.daedafusion.jetcd.EtcdClientException;
import com.daedafusion.jetcd.EtcdClientFactory;
import com.daedafusion.jetcd.EtcdNode;
import com.daedafusion.jetcd.EtcdResult;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * 支持集群etcd服务操作，多个url采用轮询机制
 * 
 * @author Administrator
 *
 */
public class ClusterEtcdClientImpl implements EtcdClient {
	private static final Logger log = Logger.getLogger(ClusterEtcdClientImpl.class);

	private final AtomicInteger index = new AtomicInteger(0);

	private final List<EtcdClient> etcdClients = new ArrayList<EtcdClient>();

	public ClusterEtcdClientImpl(String uri) {
		log.info("Initilizing ClusterEtcdClient[" + uri + "]");
		StringTokenizer st = new StringTokenizer(uri, ",");
		while (st.hasMoreElements()) {
			String s = st.nextToken();
			log.info("---Initilizing EtcdClient[" + s + "]");
			etcdClients.add(EtcdClientFactory.newInstance(s));
		}
	}

	public EtcdClient nextClient() {
		if (etcdClients.size() == 0) {
			return null;
		}
		int thisIndex = Math.abs(index.getAndIncrement());
		return etcdClients.get(thisIndex % etcdClients.size());
	}

	@Override
	public EtcdResult get(String key) throws EtcdClientException {

		return this.nextClient().get(key);
	}

	@Override
	public EtcdResult delete(String key) throws EtcdClientException {

		return this.nextClient().delete(key);
	}

	@Override
	public EtcdResult set(String key, String value) throws EtcdClientException {

		return this.nextClient().set(key, value);
	}

	@Override
	public EtcdResult set(String key, String value, Integer ttl) throws EtcdClientException {

		return this.nextClient().set(key, value, ttl);
	}

	@Override
	public EtcdResult refreshDirectory(String key, Integer ttl) throws EtcdClientException {

		return this.nextClient().refreshDirectory(key, ttl);
	}

	@Override
	public EtcdResult createDirectory(String key) throws EtcdClientException {

		return this.nextClient().createDirectory(key);
	}

	@Override
	public EtcdResult createDirectory(String key, Integer ttl) throws EtcdClientException {

		return this.nextClient().createDirectory(key, ttl);
	}

	@Override
	public List<EtcdNode> listDirectory(String key) throws EtcdClientException {

		return this.nextClient().listDirectory(key);
	}

	@Override
	public EtcdResult deleteDirectory(String key) throws EtcdClientException {

		return this.nextClient().deleteDirectory(key);
	}

	@Override
	public EtcdResult deleteDirectoryRecursive(String key) throws EtcdClientException {

		return this.nextClient().deleteDirectoryRecursive(key);
	}

	@Override
	public EtcdResult compareAndSet(String key, String prevValue, String value) throws EtcdClientException {

		return this.nextClient().compareAndSet(key, prevValue, value);
	}

	@Override
	public EtcdResult compareAndSet(String key, Integer prevIndex, String value) throws EtcdClientException {

		return this.nextClient().compareAndSet(key, prevIndex, value);
	}

	@Override
	public EtcdResult compareAndDelete(String key, String prevValue) throws EtcdClientException {

		return this.nextClient().compareAndDelete(key, prevValue);
	}

	@Override
	public EtcdResult compareAndDelete(String key, Integer prevIndex) throws EtcdClientException {

		return this.nextClient().compareAndDelete(key, prevIndex);
	}

	@Override
	public ListenableFuture<EtcdResult> watch(String key) throws EtcdClientException {

		return this.nextClient().watch(key);
	}

	@Override
	public ListenableFuture<EtcdResult> watch(String key, boolean recursive) throws EtcdClientException {

		return this.nextClient().watch(key, recursive);
	}

	@Override
	public ListenableFuture<EtcdResult> watch(String key, Long index, boolean recursive) throws EtcdClientException {

		return this.nextClient().watch(key, index, recursive);
	}

	@Override
	public EtcdResult queue(String key, String value) throws EtcdClientException {

		return this.nextClient().queue(key, value);
	}

	@Override
	public EtcdResult getQueue(String key) throws EtcdClientException {

		return this.nextClient().getQueue(key);
	}

	@Override
	public void close() throws IOException {

		this.nextClient().close();
	}
}
