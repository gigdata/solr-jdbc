package net.unit8.solr.jdbc.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.concurrent.Executor;



public class CommonsHttpConnectionImpl extends SolrConnection {
	private int timeout = 0;
	
	public CommonsHttpConnectionImpl(String serverUrl) throws MalformedURLException {
		super(serverUrl);
		HttpClient httpClient = new DefaultHttpClient();
		if (StringUtils.startsWith(serverUrl, "chss:")) {
			serverUrl = serverUrl.replace("chss:", "");
		}
		SolrServer solrServer = new HttpSolrServer(serverUrl, httpClient);
		setSolrServer(solrServer);
	}

    public static boolean accept(String url) {
        return StringUtils.startsWith(url, "http://") || StringUtils.startsWith(url, "chss:");
    }

    @Override
	public void close() {
		
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
