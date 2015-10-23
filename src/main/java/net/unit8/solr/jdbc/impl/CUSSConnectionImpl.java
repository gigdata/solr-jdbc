package net.unit8.solr.jdbc.impl;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;



public class CUSSConnectionImpl extends SolrConnection {
	private int timeout = 0;
	private int queueSize = 1000;
	private int threadCount = 10;
	
	public CUSSConnectionImpl(String serverUrl) throws MalformedURLException, URISyntaxException {
		super(serverUrl);
		//HttpClient httpClient = new HttpClient();
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		if (StringUtils.startsWith(serverUrl, "cuss:")) {
			serverUrl = serverUrl.replace("cuss:", "");
			int endIOfUrl = serverUrl.length() - 1;
			if ((endIOfUrl = StringUtils.indexOf(serverUrl, "?")) > 0) {
				URI uri = new URI(serverUrl);
				params = URLEncodedUtils.parse(uri, "UTF-8");
				for(NameValuePair nvp : params) {
					if (StringUtils.endsWithIgnoreCase(nvp.getName(), "queueSize")) {
						queueSize = Integer.parseInt(nvp.getValue());
					} else if (StringUtils.endsWithIgnoreCase(nvp.getName(), "threadCount")) {
						threadCount = Integer.parseInt(nvp.getValue());
					}
				}
			}
			//cut server URL from 0 to indexOf("?") or EOS
			serverUrl = serverUrl.substring(0, endIOfUrl);
		}
		SolrServer solrServer = new ConcurrentUpdateSolrServer(serverUrl, queueSize, threadCount);
		setSolrServer(solrServer);
	}

    public static boolean accept(String url) {
        return StringUtils.startsWith(url, "cuss:");
    }

    @Override
	public void close() {
    	//getSolrServer().shutdown();
	}

	@Override
	public int getQueryTimeout() {
		return timeout;
	}
	
	@Override
	public void setQueryTimeout(int timeout) {
		this.timeout = timeout;
		((HttpSolrServer)getSolrServer()).setConnectionTimeout(timeout*1000);
		((HttpSolrServer)getSolrServer()).setSoTimeout(timeout*1000);
	}

	@Override
	public void abort(Executor arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setSchema(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}
}
